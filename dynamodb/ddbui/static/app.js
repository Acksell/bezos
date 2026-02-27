// DynamoDB Debug UI Application

(function() {
    'use strict';

    // State
    let state = {
        tables: [],
        currentTable: null,
        currentSchema: null,
        items: [],
        lastKey: null,
        loading: false
    };

    // Pattern parsing utilities
    function parsePattern(pattern) {
        // Parse pattern like "USER#{id}" into { prefix: "USER#", variables: ["id"], regex: /^USER#(.+)$/ }
        if (!pattern) return null;
        
        const parts = [];
        const variables = [];
        let regex = '^';
        let lastEnd = 0;
        
        const varRegex = /#\{(\w+)\}/g;
        let match;
        
        while ((match = varRegex.exec(pattern)) !== null) {
            const prefix = pattern.slice(lastEnd, match.index);
            if (prefix) {
                parts.push({ type: 'static', value: prefix });
                regex += escapeRegex(prefix);
            }
            parts.push({ type: 'variable', name: match[1] });
            variables.push(match[1]);
            regex += '(.+?)';
            lastEnd = match.index + match[0].length;
        }
        
        const suffix = pattern.slice(lastEnd);
        if (suffix) {
            parts.push({ type: 'static', value: suffix });
            regex += escapeRegex(suffix);
        }
        regex += '$';
        
        return {
            pattern,
            parts,
            variables,
            regex: new RegExp(regex),
            isStatic: variables.length === 0
        };
    }

    function escapeRegex(str) {
        return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    }

    function matchPattern(parsedPattern, value) {
        if (!parsedPattern || !value) return null;
        const match = String(value).match(parsedPattern.regex);
        if (!match) return null;
        
        const result = {};
        parsedPattern.variables.forEach((v, i) => {
            result[v] = match[i + 1];
        });
        return result;
    }

    function buildFromPattern(parsedPattern, values) {
        if (!parsedPattern) return '';
        return parsedPattern.parts.map(p => {
            if (p.type === 'static') return p.value;
            return values[p.name] || '';
        }).join('');
    }

    // Entity detection
    function detectEntityType(item, schema) {
        if (!schema || !schema.entities) return null;
        
        const pkName = schema.table.partitionKey.name;
        const skName = schema.table.sortKey?.name;
        const pkVal = item[pkName];
        const skVal = skName ? item[skName] : null;
        
        for (const entity of schema.entities) {
            const pkParsed = parsePattern(entity.partitionKeyPattern);
            const skParsed = entity.sortKeyPattern ? parsePattern(entity.sortKeyPattern) : null;
            
            const pkMatch = matchPattern(pkParsed, pkVal);
            if (!pkMatch) continue;
            
            if (skParsed) {
                const skMatch = matchPattern(skParsed, skVal);
                if (!skMatch) continue;
            }
            
            return entity.type;
        }
        return null;
    }

    // API Client
    const api = {
        async get(path) {
            const res = await fetch('/api' + path);
            if (!res.ok) {
                const err = await res.json();
                throw new Error(err.error || 'Request failed');
            }
            return res.json();
        },

        async post(path, data) {
            const res = await fetch('/api' + path, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(data)
            });
            if (!res.ok) {
                const err = await res.json();
                throw new Error(err.error || 'Request failed');
            }
            return res.json();
        },

        async delete(path) {
            const res = await fetch('/api' + path, { method: 'DELETE' });
            if (!res.ok) {
                const err = await res.json();
                throw new Error(err.error || 'Request failed');
            }
            return res.json();
        }
    };

    // DOM Elements
    const $ = (sel) => document.querySelector(sel);
    const $$ = (sel) => document.querySelectorAll(sel);

    // Initialize
    async function init() {
        await loadTables();
        setupEventListeners();
    }

    // Load tables list
    async function loadTables() {
        try {
            const data = await api.get('/tables');
            state.tables = data.tables;
            await renderTableList();
        } catch (err) {
            console.error('Failed to load tables:', err);
        }
    }

    // Render table list in sidebar with entities pre-loaded
    async function renderTableList() {
        const list = $('#table-list');
        
        // First render table structure
        list.innerHTML = state.tables.map(t => `
            <li class="table-item">
                <a href="#" class="table-link" data-table="${t.name}">
                    ${t.name}
                </a>
                <ul class="entity-list" id="entities-${t.name}">
                    <li class="loading-entities">Loading...</li>
                </ul>
            </li>
        `).join('');
        
        // Then load entities for each table
        for (const t of state.tables) {
            try {
                const schema = await api.get(`/tables/${t.name}`);
                const entityList = $(`#entities-${t.name}`);
                if (entityList) {
                    const entities = schema.entities || [];
                    if (entities.length > 0) {
                        entityList.innerHTML = entities.map(e => `
                            <li>
                                <a href="#" class="entity-link" data-table="${t.name}" data-entity="${e.type}">
                                    ${e.type}
                                </a>
                            </li>
                        `).join('');
                    } else {
                        entityList.innerHTML = '<li class="no-entities">No entities</li>';
                    }
                }
            } catch (err) {
                console.error(`Failed to load entities for ${t.name}:`, err);
                const entityList = $(`#entities-${t.name}`);
                if (entityList) {
                    entityList.innerHTML = '<li class="no-entities">Failed to load</li>';
                }
            }
        }
    }

    // Render entities under a table in sidebar (for refresh)
    async function renderTableEntities(tableName) {
        // Already loaded in renderTableList, this is now a no-op
        // but kept for compatibility
    }

    // Select a table
    async function selectTable(tableName, options = {}) {
        const { entity = null, switchToQuery = false } = options;
        
        // Update active state
        $$('.table-list .table-link').forEach(a => a.classList.remove('active'));
        $$('.table-list .entity-link').forEach(a => a.classList.remove('active'));
        
        if (entity) {
            const entityLink = $(`.entity-link[data-table="${tableName}"][data-entity="${entity}"]`);
            if (entityLink) entityLink.classList.add('active');
        } else {
            const tableLink = $(`.table-link[data-table="${tableName}"]`);
            if (tableLink) tableLink.classList.add('active');
        }

        // Load table schema
        try {
            const needsLoad = state.currentTable !== tableName;
            state.currentTable = tableName;
            
            if (needsLoad) {
                state.currentSchema = await api.get(`/tables/${tableName}`);
                state.items = [];
                state.lastKey = null;
            }

            // Update UI
            $('#table-name').textContent = entity ? `${tableName} / ${entity}` : tableName;
            $('#welcome-view').classList.remove('active');
            $('#table-view').classList.add('active');

            // Render schema
            renderSchema();

            // Populate query index dropdown
            populateIndexDropdown();
            
            // Populate entity dropdown for queries
            populateEntityDropdown();
            
            // Render entities in sidebar
            await renderTableEntities(tableName);

            // If entity selected, switch to query tab and select entity
            if (switchToQuery && entity) {
                // Switch to query tab
                $$('.tab').forEach(t => t.classList.remove('active'));
                $$('.tab-content').forEach(c => c.classList.remove('active'));
                $('.tab[data-tab="query"]').classList.add('active');
                $('#tab-query').classList.add('active');
                
                // Select the entity in dropdown
                const entitySelect = $('#query-entity');
                if (entitySelect) {
                    entitySelect.value = entity;
                    renderEntityQueryFields(entity);
                }
            } else if (needsLoad) {
                // Load data only when switching tables (not entities)
                await loadData();
            }
        } catch (err) {
            console.error('Failed to load table:', err);
            alert('Failed to load table: ' + err.message);
        }
    }

    // Render schema tab
    function renderSchema() {
        const schema = state.currentSchema;

        // Build indexes table (Primary + GSIs as columns)
        const gsis = schema.table.gsis || [];
        let indexHtml = `
            <table class="index-table">
                <thead>
                    <tr>
                        <th></th>
                        <th>Primary</th>
                        ${gsis.map(g => `<th>${g.name}</th>`).join('')}
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td class="row-label">PK</td>
                        <td><code>${schema.table.partitionKey.name}</code> <span class="key-type">${schema.table.partitionKey.kind}</span></td>
                        ${gsis.map(g => `<td><code>${g.partitionKey.name}</code> <span class="key-type">${g.partitionKey.kind}</span></td>`).join('')}
                    </tr>
                    <tr>
                        <td class="row-label">SK</td>
                        <td>${schema.table.sortKey ? `<code>${schema.table.sortKey.name}</code> <span class="key-type">${schema.table.sortKey.kind}</span>` : '<span class="no-key">—</span>'}</td>
                        ${gsis.map(g => `<td>${g.sortKey ? `<code>${g.sortKey.name}</code> <span class="key-type">${g.sortKey.kind}</span>` : '<span class="no-key">—</span>'}</td>`).join('')}
                    </tr>
                </tbody>
            </table>
        `;
        $('#schema-indexes').innerHTML = indexHtml;

        // Entities
        if (schema.entities && schema.entities.length > 0) {
            $('#schema-entities').innerHTML = schema.entities.map(entity => `
                <div class="schema-card">
                    <h4>${entity.type}${entity.isVersioned ? ' (versioned)' : ''}</h4>
                    <div class="schema-row">
                        <span class="schema-label">PK Pattern:</span>
                        <span class="schema-value">${entity.partitionKeyPattern}</span>
                    </div>
                    ${entity.sortKeyPattern ? `
                    <div class="schema-row">
                        <span class="schema-label">SK Pattern:</span>
                        <span class="schema-value">${entity.sortKeyPattern}</span>
                    </div>` : ''}
                    ${entity.gsiMappings && entity.gsiMappings.length > 0 ? `
                    <div class="entity-fields">
                        <div class="schema-row">
                            <span class="schema-label">GSI Mappings:</span>
                        </div>
                        ${entity.gsiMappings.map(m => `
                        <div class="schema-row" style="margin-left: 1rem;">
                            <span class="schema-label">${m.gsi}:</span>
                            <span class="schema-value">${m.partitionPattern}${m.sortPattern ? ' / ' + m.sortPattern : ''}</span>
                        </div>
                        `).join('')}
                    </div>` : ''}
                    ${entity.fields && entity.fields.length > 0 ? `
                    <div class="entity-fields">
                        <div class="schema-row">
                            <span class="schema-label">Fields:</span>
                        </div>
                        <div class="field-list">
                            ${entity.fields.map(f => `
                                <span class="field-tag" title="${f.type}">${f.tag || f.name}</span>
                            `).join('')}
                        </div>
                    </div>` : ''}
                </div>
            `).join('');
        } else {
            $('#schema-entities').innerHTML = '<p class="placeholder">No entities defined</p>';
        }
    }

    // Populate index dropdown for queries
    function populateIndexDropdown() {
        const select = $('#query-index');
        select.innerHTML = '<option value="">Primary Index</option>';
        
        const gsis = state.currentSchema?.table?.gsis || [];
        gsis.forEach(gsi => {
            select.innerHTML += `<option value="${gsi.name}">${gsi.name}</option>`;
        });
    }

    // Populate entity dropdown for queries and render dynamic form
    function populateEntityDropdown() {
        const select = $('#query-entity');
        if (!select) return;
        
        const entities = state.currentSchema?.entities || [];
        select.innerHTML = '<option value="">-- Select Entity --</option>';
        entities.forEach(e => {
            select.innerHTML += `<option value="${e.type}">${e.type}</option>`;
        });
        
        // Clear the dynamic fields
        renderEntityQueryFields(null);
    }

    // Render entity query form fields based on selected entity
    function renderEntityQueryFields(entityType) {
        const container = $('#entity-query-fields');
        if (!container) return;
        
        if (!entityType) {
            container.innerHTML = '<p class="placeholder">Select an entity to see query fields</p>';
            return;
        }
        
        const entity = state.currentSchema?.entities?.find(e => e.type === entityType);
        if (!entity) {
            container.innerHTML = '<p class="placeholder">Entity not found</p>';
            return;
        }
        
        const pkParsed = parsePattern(entity.partitionKeyPattern);
        const skParsed = entity.sortKeyPattern ? parsePattern(entity.sortKeyPattern) : null;
        
        let html = '';
        
        // Pattern preview
        html += `<div class="pattern-preview">`;
        html += `<div class="pattern-item"><span class="pattern-label">PK:</span> <code>${entity.partitionKeyPattern}</code></div>`;
        if (entity.sortKeyPattern) {
            html += `<div class="pattern-item"><span class="pattern-label">SK:</span> <code>${entity.sortKeyPattern}</code></div>`;
        }
        html += `</div>`;
        
        // Collect PK variables
        const pkVars = pkParsed?.variables || [];
        // Collect SK variables (excluding those already in PK)
        const skVars = (skParsed?.variables || []).filter(v => !pkVars.includes(v));
        
        // If patterns are static, show simple message
        if (pkVars.length === 0 && skVars.length === 0) {
            if (pkParsed?.isStatic) {
                html += `<p class="info-text">PK is static: <code>${entity.partitionKeyPattern}</code></p>`;
            }
            if (skParsed?.isStatic) {
                html += `<p class="info-text">SK is static: <code>${entity.sortKeyPattern}</code></p>`;
            }
        } else {
            // Partition Key variables section
            if (pkVars.length > 0) {
                html += `<div class="query-section-group">`;
                html += `<h4 class="query-section-title">Partition Key</h4>`;
                html += `<div class="query-fields-grid">`;
                for (const varName of pkVars) {
                    html += `
                        <div class="form-group">
                            <label for="qv-${varName}">${varName}:</label>
                            <input type="text" id="qv-${varName}" class="query-var" data-var="${varName}" placeholder="Enter ${varName}">
                        </div>
                    `;
                }
                html += `</div></div>`;
            }
            
            // Sort Key variables section (if any unique to SK)
            if (skVars.length > 0) {
                html += `<div class="query-section-group">`;
                html += `<h4 class="query-section-title">Sort Key</h4>`;
                html += `<div class="query-fields-grid">`;
                for (const varName of skVars) {
                    html += `
                        <div class="form-group">
                            <label for="qv-${varName}">${varName}:</label>
                            <input type="text" id="qv-${varName}" class="query-var" data-var="${varName}" placeholder="Enter ${varName}">
                        </div>
                    `;
                }
                html += `</div></div>`;
            }
        }
        
        // Sort key operation (if entity has SK)
        if (skParsed) {
            html += `
                <div class="sk-options">
                    <div class="form-group">
                        <label for="query-sk-mode">Sort Key Query Mode:</label>
                        <select id="query-sk-mode">
                            <option value="exact">Exact Match</option>
                            <option value="begins_with">Begins With (prefix)</option>
                            <option value="none">Any (no SK condition)</option>
                        </select>
                    </div>
                </div>
            `;
        }
        
        html += `<button id="btn-entity-query" class="btn btn-primary">Run Query</button>`;
        
        container.innerHTML = html;
        
        // Re-attach event listener for the button
        const btn = $('#btn-entity-query');
        if (btn) {
            btn.addEventListener('click', runEntityQuery);
        }
    }

    // Run query based on entity form
    async function runEntityQuery() {
        const entityType = $('#query-entity')?.value;
        if (!entityType) {
            alert('Please select an entity');
            return;
        }
        
        const entity = state.currentSchema?.entities?.find(e => e.type === entityType);
        if (!entity) return;
        
        const pkParsed = parsePattern(entity.partitionKeyPattern);
        const skParsed = entity.sortKeyPattern ? parsePattern(entity.sortKeyPattern) : null;
        
        // Gather variable values
        const varValues = {};
        document.querySelectorAll('.query-var').forEach(input => {
            varValues[input.dataset.var] = input.value;
        });
        
        // Build the actual PK value
        const pkVal = buildFromPattern(pkParsed, varValues);
        if (!pkVal) {
            alert('Partition key value is required');
            return;
        }
        
        // Build query
        const schema = state.currentSchema;
        const pkName = schema.table.partitionKey.name;
        const pkKind = schema.table.partitionKey.kind;
        const skName = schema.table.sortKey?.name;
        const skKind = schema.table.sortKey?.kind;
        
        let expr = '#pk = :pk';
        const names = { '#pk': pkName };
        // Convert pk value based on key type
        const values = { ':pk': convertKeyValue(pkVal, pkKind) };
        
        // Handle sort key based on mode
        const skMode = $('#query-sk-mode')?.value || 'none';
        if (skParsed && skName && skMode !== 'none') {
            const skVal = buildFromPattern(skParsed, varValues);
            names['#sk'] = skName;
            // Convert sk value based on key type
            values[':sk'] = convertKeyValue(skVal, skKind);
            
            if (skMode === 'exact') {
                expr += ' AND #sk = :sk';
            } else if (skMode === 'begins_with') {
                expr += ' AND begins_with(#sk, :sk)';
            }
        }
        
        const body = {
            keyConditionExpression: expr,
            expressionAttributeNames: names,
            expressionAttributeValues: values,
            limit: 50
        };
        
        console.log('Query body:', JSON.stringify(body, null, 2));
        
        try {
            const data = await api.post(`/tables/${state.currentTable}/query`, body);
            renderQueryResults(data.items);
        } catch (err) {
            console.error('Query failed:', err);
            alert('Query failed: ' + err.message);
        }
    }
    
    // Convert a key value based on the key kind (S, N, B)
    function convertKeyValue(value, kind) {
        if (!value) return value;
        switch (kind) {
            case 'N':
                // Number - return as-is, backend will convert
                return value;
            case 'B':
                // Binary - encode as base64
                // The value is already a string, encode it to base64
                return btoa(value);
            case 'S':
            default:
                return value;
        }
    }

    // Load data (scan)
    async function loadData(append = false) {
        if (state.loading) return;
        
        const limit = parseInt($('#page-limit').value);
        const params = new URLSearchParams({ limit });
        
        if (append && state.lastKey) {
            params.set('lastKey', state.lastKey);
        }

        state.loading = true;
        try {
            const data = await api.get(`/tables/${state.currentTable}/items?${params}`);
            
            if (append) {
                state.items = [...state.items, ...data.items];
            } else {
                state.items = data.items;
            }
            state.lastKey = data.lastKey || null;
            
            renderData();
        } catch (err) {
            console.error('Failed to load data:', err);
            alert('Failed to load data: ' + err.message);
        } finally {
            state.loading = false;
        }
    }

    // Render data table
    function renderData() {
        const schema = state.currentSchema;
        const pkName = schema.table.partitionKey.name;
        const skName = schema.table.sortKey?.name;

        // Pre-compute entity types for all items
        const itemsWithEntity = state.items.map(item => ({
            item,
            entityType: detectEntityType(item, schema)
        }));

        // Get all unique keys from items
        const allKeys = new Set();
        state.items.forEach(item => {
            Object.keys(item).forEach(k => allKeys.add(k));
        });

        // Sort keys: pk first, sk second, then alphabetically
        const sortedKeys = Array.from(allKeys).sort((a, b) => {
            if (a === pkName) return -1;
            if (b === pkName) return 1;
            if (a === skName) return -1;
            if (b === skName) return 1;
            return a.localeCompare(b);
        });

        // Add Entity as first column
        const columns = ['_entity', ...sortedKeys];

        // Render header
        $('#data-thead').innerHTML = `
            <tr>
                ${columns.map(k => `<th>${k === '_entity' ? 'Entity' : k}</th>`).join('')}
            </tr>
        `;

        // Render rows
        if (state.items.length === 0) {
            $('#data-tbody').innerHTML = `
                <tr>
                    <td colspan="${columns.length}" class="empty-state">
                        No items found
                    </td>
                </tr>
            `;
        } else {
            $('#data-tbody').innerHTML = itemsWithEntity.map(({ item, entityType }, idx) => `
                <tr data-index="${idx}">
                    <td><span class="entity-badge" data-entity="${entityType || 'unknown'}">${entityType || '?'}</span></td>
                    ${sortedKeys.map(k => {
                        const val = item[k];
                        const isKey = k === pkName || k === skName;
                        const display = formatValue(val);
                        return `<td class="${isKey ? 'key-cell' : ''}" title="${escapeHtml(JSON.stringify(val))}">${display}</td>`;
                    }).join('')}
                </tr>
            `).join('');
        }

        // Update count and load more button
        $('#item-count').textContent = `${state.items.length} items`;
        $('#btn-load-more').style.display = state.lastKey ? 'inline-block' : 'none';
    }

    // Format a value for display
    function formatValue(val) {
        if (val === null || val === undefined) {
            return '<span style="color: var(--text-secondary)">null</span>';
        }
        if (typeof val === 'object') {
            return escapeHtml(JSON.stringify(val));
        }
        return escapeHtml(String(val));
    }

    // Escape HTML
    function escapeHtml(str) {
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    // Open item modal
    function openItemModal(item = null, isNew = false) {
        const modal = $('#item-modal');
        const editor = $('#item-editor');
        const title = $('#modal-title');
        const deleteBtn = $('#btn-delete-item');

        if (isNew) {
            title.textContent = 'New Item';
            deleteBtn.style.display = 'none';
            
            // Pre-populate with key structure
            const schema = state.currentSchema;
            const template = {};
            template[schema.table.partitionKey.name] = '';
            if (schema.table.sortKey) {
                template[schema.table.sortKey.name] = '';
            }
            editor.value = JSON.stringify(template, null, 2);
        } else {
            title.textContent = 'Edit Item';
            deleteBtn.style.display = 'inline-block';
            editor.value = JSON.stringify(item, null, 2);
        }

        modal.classList.add('active');
        editor.focus();
    }

    // Close item modal
    function closeItemModal() {
        $('#item-modal').classList.remove('active');
    }

    // Save item
    async function saveItem() {
        const editor = $('#item-editor');
        let item;
        
        try {
            item = JSON.parse(editor.value);
        } catch (err) {
            alert('Invalid JSON: ' + err.message);
            return;
        }

        try {
            await api.post(`/tables/${state.currentTable}/items`, { item });
            closeItemModal();
            await loadData();
        } catch (err) {
            alert('Failed to save item: ' + err.message);
        }
    }

    // Delete item
    async function deleteItem() {
        const editor = $('#item-editor');
        let item;
        
        try {
            item = JSON.parse(editor.value);
        } catch (err) {
            alert('Invalid JSON: ' + err.message);
            return;
        }

        const schema = state.currentSchema;
        const pk = item[schema.table.partitionKey.name];
        const sk = schema.table.sortKey ? item[schema.table.sortKey.name] : null;

        if (!pk) {
            alert('Partition key is required');
            return;
        }

        if (!confirm('Are you sure you want to delete this item?')) {
            return;
        }

        try {
            const path = sk 
                ? `/tables/${state.currentTable}/items/${encodeURIComponent(pk)}/${encodeURIComponent(sk)}`
                : `/tables/${state.currentTable}/items/${encodeURIComponent(pk)}`;
            await api.delete(path);
            closeItemModal();
            await loadData();
        } catch (err) {
            alert('Failed to delete item: ' + err.message);
        }
    }

    // Run query
    async function runQuery() {
        const indexName = $('#query-index').value;
        const pk = $('#query-pk').value;
        const skOp = $('#query-sk-op').value;
        const skVal = $('#query-sk-val').value;
        const skVal2 = $('#query-sk-val2').value;

        if (!pk) {
            alert('Partition key value is required');
            return;
        }

        const schema = state.currentSchema;
        let keyDefs;
        
        if (indexName) {
            const gsi = schema.table.gsis.find(g => g.name === indexName);
            keyDefs = { pk: gsi.partitionKey, sk: gsi.sortKey };
        } else {
            keyDefs = { pk: schema.table.partitionKey, sk: schema.table.sortKey };
        }

        // Build key condition expression
        let expr = `#pk = :pk`;
        const names = { '#pk': keyDefs.pk.name };
        const values = { ':pk': pk };

        if (skOp && skVal && keyDefs.sk) {
            names['#sk'] = keyDefs.sk.name;
            values[':sk'] = skVal;

            switch (skOp) {
                case '=':
                    expr += ' AND #sk = :sk';
                    break;
                case 'begins_with':
                    expr += ' AND begins_with(#sk, :sk)';
                    break;
                case '<':
                    expr += ' AND #sk < :sk';
                    break;
                case '<=':
                    expr += ' AND #sk <= :sk';
                    break;
                case '>':
                    expr += ' AND #sk > :sk';
                    break;
                case '>=':
                    expr += ' AND #sk >= :sk';
                    break;
                case 'between':
                    values[':sk2'] = skVal2;
                    expr += ' AND #sk BETWEEN :sk AND :sk2';
                    break;
            }
        }

        const body = {
            keyConditionExpression: expr,
            expressionAttributeNames: names,
            expressionAttributeValues: values,
            limit: 50
        };

        try {
            const path = indexName 
                ? `/tables/${state.currentTable}/gsi/${indexName}/query`
                : `/tables/${state.currentTable}/query`;
            const data = await api.post(path, body);
            renderQueryResults(data.items);
        } catch (err) {
            console.error('Query failed:', err);
            alert('Query failed: ' + err.message);
        }
    }

    // Render query results
    function renderQueryResults(items) {
        const container = $('#query-results-container');
        
        if (items.length === 0) {
            container.innerHTML = '<p class="placeholder">No items found</p>';
            return;
        }

        container.innerHTML = `
            <p>${items.length} item(s) found</p>
            <div class="json-display">${escapeHtml(JSON.stringify(items, null, 2))}</div>
        `;
    }

    // Setup event listeners
    function setupEventListeners() {
        // Table selection
        $('#table-list').addEventListener('click', (e) => {
            const entityLink = e.target.closest('a.entity-link');
            if (entityLink) {
                e.preventDefault();
                selectTable(entityLink.dataset.table, { 
                    entity: entityLink.dataset.entity, 
                    switchToQuery: true 
                });
                return;
            }
            
            const tableLink = e.target.closest('a.table-link');
            if (tableLink) {
                e.preventDefault();
                selectTable(tableLink.dataset.table);
            }
        });

        // Tabs
        $$('.tab').forEach(tab => {
            tab.addEventListener('click', () => {
                const tabName = tab.dataset.tab;
                $$('.tab').forEach(t => t.classList.remove('active'));
                $$('.tab-content').forEach(c => c.classList.remove('active'));
                tab.classList.add('active');
                $(`#tab-${tabName}`).classList.add('active');
            });
        });

        // Refresh button
        $('#btn-refresh').addEventListener('click', () => loadData());

        // New item button
        $('#btn-new-item').addEventListener('click', () => openItemModal(null, true));

        // Page limit change
        $('#page-limit').addEventListener('change', () => loadData());

        // Load more button
        $('#btn-load-more').addEventListener('click', () => loadData(true));

        // Row click to edit
        $('#data-tbody').addEventListener('click', (e) => {
            const row = e.target.closest('tr[data-index]');
            if (row) {
                const idx = parseInt(row.dataset.index);
                openItemModal(state.items[idx]);
            }
        });

        // Modal close buttons
        $$('.modal-close').forEach(btn => {
            btn.addEventListener('click', closeItemModal);
        });

        // Modal backdrop click
        $('#item-modal').addEventListener('click', (e) => {
            if (e.target === e.currentTarget) {
                closeItemModal();
            }
        });

        // Save item
        $('#btn-save-item').addEventListener('click', saveItem);

        // Delete item
        $('#btn-delete-item').addEventListener('click', deleteItem);

        // Sort key operator change (for advanced query)
        const skOpSelect = $('#query-sk-op');
        if (skOpSelect) {
            skOpSelect.addEventListener('change', (e) => {
                const val2 = $('#query-sk-val2');
                if (val2) val2.style.display = e.target.value === 'between' ? 'block' : 'none';
            });
        }

        // Run query (advanced)
        const runQueryBtn = $('#btn-run-query');
        if (runQueryBtn) {
            runQueryBtn.addEventListener('click', runQuery);
        }
        
        // Query entity selection - update sidebar and breadcrumbs too
        document.addEventListener('change', (e) => {
            if (e.target.id === 'query-entity') {
                const entityType = e.target.value;
                renderEntityQueryFields(entityType);
                
                // Update sidebar highlighting
                $$('.table-list .entity-link').forEach(a => a.classList.remove('active'));
                if (entityType && state.currentTable) {
                    const entityLink = $(`.entity-link[data-table="${state.currentTable}"][data-entity="${entityType}"]`);
                    if (entityLink) entityLink.classList.add('active');
                    // Update breadcrumb
                    $('#table-name').textContent = `${state.currentTable} / ${entityType}`;
                } else if (state.currentTable) {
                    // Reset to table only
                    const tableLink = $(`.table-link[data-table="${state.currentTable}"]`);
                    if (tableLink) tableLink.classList.add('active');
                    $('#table-name').textContent = state.currentTable;
                }
            }
        });

        // Keyboard shortcuts
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                closeItemModal();
            }
            if (e.key === 's' && (e.ctrlKey || e.metaKey)) {
                if ($('#item-modal').classList.contains('active')) {
                    e.preventDefault();
                    saveItem();
                }
            }
        });
    }

    // Start
    document.addEventListener('DOMContentLoaded', init);
})();

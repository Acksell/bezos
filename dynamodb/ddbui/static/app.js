// DynamoDB Debug UI Application

(function() {
    'use strict';

    // State
    let state = {
        tables: [],
        currentTable: null,
        currentSchema: null,
        items: [],
        queryItems: [],
        lastKey: null,
        loading: false,
        selectedIndices: new Set(),
        selectedQueryIndices: new Set(),
        selectedQueryIndex: '',  // currently selected index for entity query ('' = primary)
        scanEntityFilter: new Set(),  // entity types to show (empty = all, client-side)
        scanFilterRows: [],      // server-side filter rows [{attribute, condition, type, value, value2}]
        scanProjection: '',      // projection expression for scan (comma-separated attribute names)
        queryFilterRows: [],     // server-side filter rows for query tab
        queryProjection: '',     // projection expression for query tab
        queryLastKey: null,      // pagination key for query results
        serverInfo: null,        // { mode: "local" | "aws", region?, profile?, endpoint? }
    };

    // Per-table filter state cache: tableName -> { scanEntityFilter (Set), scanFilterRows, scanProjection }
    const tableFilterCache = {};

    function saveTableFilters() {
        if (!state.currentTable) return;
        syncFilterRowState();
        tableFilterCache[state.currentTable] = {
            scanEntityFilter: new Set(state.scanEntityFilter),
            scanFilterRows: state.scanFilterRows.map(r => ({ ...r })),
            scanProjection: state.scanProjection,
        };
    }

    function restoreTableFilters(tableName) {
        const cached = tableFilterCache[tableName];
        if (cached) {
            state.scanEntityFilter = new Set(cached.scanEntityFilter);
            state.scanFilterRows = cached.scanFilterRows.map(r => ({ ...r }));
            state.scanProjection = cached.scanProjection;
        } else {
            state.scanEntityFilter = new Set();
            state.scanFilterRows = [];
            state.scanProjection = '';
        }
    }

    // Pattern utilities - uses parsed patterns from backend
    // The backend provides patterns as arrays of parts: {isLiteral, value, formats, printfSpec, fieldType}
    
    function escapeRegex(str) {
        return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    }

    // Highlight {variables} in a pattern string with light blue color
    function highlightPatternVars(pattern) {
        if (!pattern) return '';
        return pattern.replace(/\{([^}]+)\}/g, '<span class="pattern-var">{$1}</span>');
    }

    // Build a regex from parsed pattern parts (from backend)
    function buildRegexFromParsed(parsedPattern) {
        if (!parsedPattern || !parsedPattern.parts) return null;
        
        let regex = '^';
        const variables = [];
        
        for (const part of parsedPattern.parts) {
            if (part.isLiteral) {
                regex += escapeRegex(part.value);
            } else {
                // Variable - capture any characters
                variables.push(part.value);
                regex += '(.+?)';
            }
        }
        regex += '$';
        
        return {
            regex: new RegExp(regex),
            variables,
            isStatic: variables.length === 0
        };
    }

    // Match a value against a parsed pattern
    function matchParsedPattern(parsedPattern, value) {
        if (!parsedPattern || !value) return null;
        
        const regexInfo = buildRegexFromParsed(parsedPattern);
        if (!regexInfo) return null;
        
        const match = String(value).match(regexInfo.regex);
        if (!match) return null;
        
        const result = {};
        regexInfo.variables.forEach((v, i) => {
            result[v] = match[i + 1];
        });
        return result;
    }

    // Build a key value from parsed pattern and variable values
    function buildFromParsedPattern(parsedPattern, values) {
        if (!parsedPattern || !parsedPattern.parts) return '';
        
        return parsedPattern.parts.map(part => {
            if (part.isLiteral) {
                return part.value;
            }
            // Variable reference - use the value from input
            return values[part.value] || '';
        }).join('');
    }

    // Build a key value for prefix/begins_with queries - stops at first empty variable
    // This avoids the bug where "chat#{tenantID}#{id}" with empty tenantID becomes "chat##"
    // instead of just "chat#"
    function buildPrefixFromParsedPattern(parsedPattern, values) {
        if (!parsedPattern || !parsedPattern.parts) return '';
        
        let result = '';
        for (const part of parsedPattern.parts) {
            if (part.isLiteral) {
                result += part.value;
            } else {
                // Variable - if empty, stop here (don't include trailing separators)
                const val = values[part.value] || '';
                if (val === '') {
                    break;
                }
                result += val;
            }
        }
        return result;
    }

    // Get variable names from a parsed pattern
    function getVariablesFromParsed(parsedPattern) {
        if (!parsedPattern || !parsedPattern.parts) return [];
        return parsedPattern.parts
            .filter(p => !p.isLiteral)
            .map(p => ({ name: p.value, fieldType: p.fieldType, formats: p.formats }));
    }

    // Check if a parsed pattern is static (no variables)
    function isStaticPattern(parsedPattern) {
        if (!parsedPattern || !parsedPattern.parts) return true;
        return parsedPattern.parts.every(p => p.isLiteral);
    }

    // Get the static value if pattern is fully literal
    function getStaticValue(parsedPattern) {
        if (!parsedPattern || !parsedPattern.parts) return '';
        return parsedPattern.parts.map(p => p.value).join('');
    }

    // Entity detection using backend-parsed patterns
    function detectEntityType(item, schema) {
        if (!schema || !schema.entities) return null;
        
        const pkName = schema.table.partitionKey.name;
        const skName = schema.table.sortKey?.name;
        const pkVal = item[pkName];
        const skVal = skName ? item[skName] : null;
        
        for (const entity of schema.entities) {
            // Use backend-parsed patterns if available
            const pkParsed = entity.partitionKeyParsed;
            const skParsed = entity.sortKeyParsed;
            
            const pkMatch = matchParsedPattern(pkParsed, pkVal);
            if (!pkMatch) continue;
            
            if (skParsed && !isStaticPattern(skParsed)) {
                const skMatch = matchParsedPattern(skParsed, skVal);
                if (!skMatch) continue;
            } else if (skParsed && isStaticPattern(skParsed)) {
                // Static SK - must match exactly
                if (skVal !== getStaticValue(skParsed)) continue;
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
        await loadServerInfo();
        await loadTables();
        setupEventListeners();
    }

    // Load server info (mode: local or aws)
    async function loadServerInfo() {
        try {
            state.serverInfo = await api.get('/info');
            if (state.serverInfo.mode === 'aws') {
                const banner = $('#aws-banner');
                const detail = $('#aws-banner-detail');
                if (banner) banner.style.display = 'block';
                if (detail) {
                    const parts = [];
                    if (state.serverInfo.region) parts.push(state.serverInfo.region);
                    if (state.serverInfo.profile) parts.push('profile: ' + state.serverInfo.profile);
                    if (state.serverInfo.endpoint) parts.push(state.serverInfo.endpoint);
                    detail.textContent = parts.length > 0 ? '(' + parts.join(', ') + ')' : '';
                }
            }
        } catch (err) {
            console.error('Failed to load server info:', err);
            state.serverInfo = { mode: 'local' };
        }
    }

    // Returns true if the user confirms the write, or if we're in local mode.
    function confirmAWSWrite(action) {
        if (!state.serverInfo || state.serverInfo.mode !== 'aws') return true;
        return confirm(
            `You are about to ${action} in AWS DynamoDB` +
            (state.serverInfo.region ? ` (${state.serverInfo.region})` : '') +
            `. This will modify real data. Continue?`
        );
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

            if (needsLoad) {
                // Save current table's filter state before switching
                saveTableFilters();
                state.currentTable = tableName;
                state.currentSchema = await api.get(`/tables/${tableName}`);
                state.items = [];
                state.lastKey = null;
                state.selectedIndices.clear();
                state.selectedQueryIndex = '';
                state.queryFilterRows = [];
                state.queryProjection = '';
                state.queryLastKey = null;
                // Restore filter state for the new table
                restoreTableFilters(tableName);
            }

            // Update UI
            $('#table-name').textContent = entity ? `${tableName} / ${entity}` : tableName;
            $('#welcome-view').classList.remove('active');
            $('#table-view').classList.add('active');

            // Render schema
            renderSchema();
            
            // Populate entity dropdown for queries
            populateEntityDropdown();
            
            // Render filter rows and count (restored from cache or empty)
            renderFilterRows();
            updateFilterCount();
            
            // Restore projection input from cached state
            const projInput = $('#scan-projection');
            if (projInput) projInput.value = state.scanProjection || '';
            
            // Restore filter body visibility based on whether there are filter rows
            const filtersBody = $('#scan-filters-body');
            const arrow = $('#scan-filters-toggle .toggle-arrow');
            if (state.scanFilterRows.length > 0) {
                filtersBody.style.display = 'block';
                arrow.textContent = '\u25BC';
            } else {
                filtersBody.style.display = 'none';
                arrow.textContent = '\u25B6';
            }
            
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
            } else {
                // Switch to scan tab when clicking table name
                $$('.tab').forEach(t => t.classList.remove('active'));
                $$('.tab-content').forEach(c => c.classList.remove('active'));
                $('.tab[data-tab="data"]').classList.add('active');
                $('#tab-data').classList.add('active');
                if (needsLoad || state.items.length === 0) {
                    await loadData();
                }
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
                        <span class="schema-value">${highlightPatternVars(entity.partitionKeyPattern)}</span>
                    </div>
                    ${entity.sortKeyPattern ? `
                    <div class="schema-row">
                        <span class="schema-label">SK Pattern:</span>
                        <span class="schema-value">${highlightPatternVars(entity.sortKeyPattern)}</span>
                    </div>` : ''}
                    ${entity.gsiMappings && entity.gsiMappings.length > 0 ? `
                    <div class="entity-fields">
                        <div class="schema-row">
                            <span class="schema-label">GSI Mappings:</span>
                        </div>
                        ${entity.gsiMappings.map(m => `
                        <div class="schema-row" style="margin-left: 1rem;">
                            <span class="schema-label">${m.gsi}:</span>
                            <span class="schema-value">${highlightPatternVars(m.partitionPattern)}${m.sortPattern ? ' / ' + highlightPatternVars(m.sortPattern) : ''}</span>
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

    // Populate entity dropdown for queries and render dynamic form
    function populateEntityDropdown() {
        const select = $('#query-entity');
        if (!select) return;
        
        const entities = state.currentSchema?.entities || [];
        select.innerHTML = '<option value="__any__">-- Any --</option>';
        entities.forEach(e => {
            select.innerHTML += `<option value="${e.type}">${e.type}</option>`;
        });
        
        // Render the "any" query fields by default
        renderEntityQueryFields('__any__');
    }

    // Populate entity filter dropdown in scan toolbar
    // Entity filter is now embedded in the table header (popover in renderData)

    // Render entity query form fields based on selected entity
    function renderEntityQueryFields(entityType) {
        const container = $('#entity-query-fields');
        if (!container) return;
        
        if (!entityType) {
            container.innerHTML = '<p class="placeholder">Select an entity to see query fields</p>';
            return;
        }
        
        // "Any" mode - show raw query fields (like advanced query)
        if (entityType === '__any__') {
            renderAnyQueryFields(container);
            return;
        }
        
        const entity = state.currentSchema?.entities?.find(e => e.type === entityType);
        if (!entity) {
            container.innerHTML = '<p class="placeholder">Entity not found</p>';
            return;
        }
        
        // Get the selected index (default: primary)
        const selectedIndex = state.selectedQueryIndex || '';
        
        // Determine which parsed patterns to use based on selected index
        let pkParsed, skParsed, pkPatternDisplay, skPatternDisplay;
        if (selectedIndex) {
            // Find the GSI mapping for this entity
            const gsiMapping = (entity.enrichedGSIMappings || []).find(m => m.gsi === selectedIndex);
            if (gsiMapping) {
                pkParsed = gsiMapping.partitionParsed;
                skParsed = gsiMapping.sortParsed;
                pkPatternDisplay = gsiMapping.partitionPattern;
                skPatternDisplay = gsiMapping.sortPattern;
            } else {
                // Entity doesn't have a mapping for this GSI
                pkParsed = null;
                skParsed = null;
                pkPatternDisplay = null;
                skPatternDisplay = null;
            }
        } else {
            // Use backend-parsed patterns for primary index
            pkParsed = entity.partitionKeyParsed;
            skParsed = entity.sortKeyParsed;
            pkPatternDisplay = entity.partitionKeyPattern;
            skPatternDisplay = entity.sortKeyPattern;
        }
        
        // Build list of available indexes for this entity
        const gsiMappings = entity.enrichedGSIMappings || [];
        
        let html = '';
        
        // Index selector (only show if entity has GSI mappings)
        if (gsiMappings.length > 0) {
            html += `<div class="form-group">
                <label for="entity-query-index">Index:</label>
                <select id="entity-query-index">
                    <option value="">Primary Index</option>
                    ${gsiMappings.map(m => `<option value="${m.gsi}"${m.gsi === selectedIndex ? ' selected' : ''}>${m.gsi}</option>`).join('')}
                </select>
            </div>`;
        }
        
        // If a GSI is selected but entity has no mapping for it, show message
        if (selectedIndex && !pkParsed) {
            html += `<p class="placeholder">This entity has no mapping for index "${selectedIndex}"</p>`;
            container.innerHTML = html;
            attachIndexChangeListener(entityType);
            return;
        }
        
        // Pattern preview
        if (pkPatternDisplay) {
            html += `<div class="pattern-preview">`;
            html += `<div class="pattern-item"><span class="pattern-label">PK:</span> <code>${highlightPatternVars(pkPatternDisplay)}</code></div>`;
            if (skPatternDisplay) {
                html += `<div class="pattern-item"><span class="pattern-label">SK:</span> <code>${highlightPatternVars(skPatternDisplay)}</code></div>`;
            }
            html += `</div>`;
        }
        
        // Collect PK variables using backend-parsed info
        const pkVars = getVariablesFromParsed(pkParsed);
        // Collect SK variables (excluding those already in PK by name)
        const pkVarNames = pkVars.map(v => v.name);
        const skVars = getVariablesFromParsed(skParsed).filter(v => !pkVarNames.includes(v.name));
        
        // If patterns are static, show simple message
        if (pkVars.length === 0 && skVars.length === 0) {
            if (isStaticPattern(pkParsed)) {
                html += `<p class="info-text">PK is static: <code>${pkPatternDisplay}</code></p>`;
            }
            if (isStaticPattern(skParsed)) {
                html += `<p class="info-text">SK is static: <code>${skPatternDisplay}</code></p>`;
            }
        } else {
            // Partition Key variables section
            if (pkVars.length > 0) {
                html += `<div class="query-section-group">`;
                html += `<h4 class="query-section-title">Partition Key</h4>`;
                html += `<div class="query-fields-grid">`;
                for (const varInfo of pkVars) {
                    const typeHint = varInfo.fieldType ? ` (${varInfo.fieldType})` : '';
                    html += `
                        <div class="form-group">
                            <label for="qv-${varInfo.name}">${varInfo.name}${typeHint}:</label>
                            <input type="text" id="qv-${varInfo.name}" class="query-var" data-var="${varInfo.name}" placeholder="Enter ${varInfo.name}">
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
                for (const varInfo of skVars) {
                    const typeHint = varInfo.fieldType ? ` (${varInfo.fieldType})` : '';
                    html += `
                        <div class="form-group">
                            <label for="qv-${varInfo.name}">${varInfo.name}${typeHint}:</label>
                            <input type="text" id="qv-${varInfo.name}" class="query-var" data-var="${varInfo.name}" placeholder="Enter ${varInfo.name}">
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
                            <option value="begins_with">Begins With (prefix)</option>
                            <option value="exact">Exact Match</option>
                            <option value="none">Any (no SK condition)</option>
                        </select>
                    </div>
                </div>
            `;
        }
        
        container.innerHTML = html;
        
        // Attach index change listener
        attachIndexChangeListener(entityType);
    }
    
    // Attach change listener to entity query index selector
    function attachIndexChangeListener(entityType) {
        const indexSelect = $('#entity-query-index');
        if (indexSelect) {
            indexSelect.addEventListener('change', (e) => {
                state.selectedQueryIndex = e.target.value;
                renderEntityQueryFields(entityType);
            });
        }
    }

    // Render "Any" query fields - raw PK/SK inputs with index selection
    function renderAnyQueryFields(container) {
        const schema = state.currentSchema;
        if (!schema) return;
        
        const gsis = schema.table.gsis || [];
        
        // Get key defs for current selection
        function getKeyDefs(indexName) {
            if (indexName) {
                const gsi = gsis.find(g => g.name === indexName);
                return gsi ? { pk: gsi.partitionKey, sk: gsi.sortKey } : { pk: schema.table.partitionKey, sk: schema.table.sortKey };
            }
            return { pk: schema.table.partitionKey, sk: schema.table.sortKey };
        }
        
        const selectedAnyIndex = $('#any-query-index')?.value || '';
        const keyDefs = getKeyDefs(selectedAnyIndex);
        
        let html = '';
        
        // Index selector
        html += `<div class="form-group">
            <label for="any-query-index">Index:</label>
            <select id="any-query-index">
                <option value="">Primary Index</option>
                ${gsis.map(g => `<option value="${g.name}"${g.name === selectedAnyIndex ? ' selected' : ''}>${g.name}</option>`).join('')}
            </select>
        </div>`;
        
        // PK input - show attribute name in label
        html += `<div class="form-group">
            <label for="any-query-pk">Partition Key (<code>${keyDefs.pk.name}</code>):</label>
            <input type="text" id="any-query-pk" placeholder="e.g., USER#123">
        </div>`;
        
        // SK condition - show attribute name in label
        const skLabel = keyDefs.sk ? `Sort Key (<code>${keyDefs.sk.name}</code>)` : 'Sort Key';
        html += `<div class="form-group">
            <label for="any-query-sk-op">${skLabel} Condition:</label>
            <div class="inline-group">
                <select id="any-query-sk-op">
                    <option value="">None</option>
                    <option value="=">=</option>
                    <option value="begins_with">begins_with</option>
                    <option value="<">&lt;</option>
                    <option value="<=">&lt;=</option>
                    <option value=">">&gt;</option>
                    <option value=">=">&gt;=</option>
                    <option value="between">between</option>
                </select>
                <input type="text" id="any-query-sk-val" placeholder="Sort key value">
                <input type="text" id="any-query-sk-val2" placeholder="End value" style="display:none">
            </div>
        </div>`;
        
        container.innerHTML = html;
        
        // SK operator change - show/hide second value input
        const skOpSelect = $('#any-query-sk-op');
        if (skOpSelect) {
            skOpSelect.addEventListener('change', (e) => {
                const val2 = $('#any-query-sk-val2');
                if (val2) val2.style.display = e.target.value === 'between' ? 'block' : 'none';
            });
        }
        
        // Index change - re-render to update labels
        const indexSelect = $('#any-query-index');
        if (indexSelect) {
            indexSelect.addEventListener('change', () => {
                renderAnyQueryFields(container);
            });
        }
    }

    // Run query based on entity form
    async function runEntityQuery(append = false) {
        const entityType = $('#query-entity')?.value;
        if (!entityType) {
            alert('Please select an entity');
            return;
        }
        
        // "Any" mode - run raw query
        if (entityType === '__any__') {
            await runAnyQuery(append);
            return;
        }
        
        const entity = state.currentSchema?.entities?.find(e => e.type === entityType);
        if (!entity) return;
        
        // Determine which index and patterns to use
        const selectedIndex = state.selectedQueryIndex || '';
        let pkParsed, skParsed, pkName, pkKind, skName, skKind;
        
        if (selectedIndex) {
            // Use GSI mapping patterns
            const gsiMapping = (entity.enrichedGSIMappings || []).find(m => m.gsi === selectedIndex);
            if (!gsiMapping) {
                alert('No GSI mapping found for ' + selectedIndex);
                return;
            }
            pkParsed = gsiMapping.partitionParsed;
            skParsed = gsiMapping.sortParsed;
            
            // Look up the GSI key attribute names from the table schema
            const gsi = state.currentSchema.table.gsis.find(g => g.name === selectedIndex);
            if (!gsi) {
                alert('GSI not found: ' + selectedIndex);
                return;
            }
            pkName = gsi.partitionKey.name;
            pkKind = gsi.partitionKey.kind;
            skName = gsi.sortKey?.name;
            skKind = gsi.sortKey?.kind;
        } else {
            // Use primary index patterns
            pkParsed = entity.partitionKeyParsed;
            skParsed = entity.sortKeyParsed;
            const schema = state.currentSchema;
            pkName = schema.table.partitionKey.name;
            pkKind = schema.table.partitionKey.kind;
            skName = schema.table.sortKey?.name;
            skKind = schema.table.sortKey?.kind;
        }
        
        // Gather variable values
        const varValues = {};
        document.querySelectorAll('.query-var').forEach(input => {
            varValues[input.dataset.var] = input.value;
        });
        
        // Build the actual PK value using backend-parsed pattern
        const pkVal = buildFromParsedPattern(pkParsed, varValues);
        if (!pkVal) {
            alert('Partition key value is required');
            return;
        }
        
        let expr = '#pk = :pk';
        const names = { '#pk': pkName };
        // Convert pk value based on key type
        const values = { ':pk': convertKeyValue(pkVal, pkKind) };
        
        // Handle sort key based on mode
        const skMode = $('#query-sk-mode')?.value || 'begins_with';
        if (skParsed && skName && skMode !== 'none') {
            // Use prefix builder for begins_with to avoid trailing separator bug
            const skVal = skMode === 'begins_with' 
                ? buildPrefixFromParsedPattern(skParsed, varValues)
                : buildFromParsedPattern(skParsed, varValues);
            names['#sk'] = skName;
            // Convert sk value based on key type
            values[':sk'] = convertKeyValue(skVal, skKind);
            
            if (skMode === 'exact') {
                expr += ' AND #sk = :sk';
            } else if (skMode === 'begins_with') {
                expr += ' AND begins_with(#sk, :sk)';
            }
        }
        
        // Sync projection state
        const projInput = $('#query-projection');
        if (projInput) state.queryProjection = projInput.value;

        const limit = parseInt($('#query-limit').value);
        const filter = buildQueryFilter();
        const projection = buildQueryProjection();

        const body = {
            keyConditionExpression: expr,
            expressionAttributeNames: names,
            expressionAttributeValues: values,
            limit: limit
        };

        // Add filter expression
        if (filter) {
            body.filterExpression = filter.filterExpression;
            Object.assign(body.expressionAttributeNames, filter.expressionAttributeNames);
            if (filter.expressionAttributeValues) {
                Object.assign(body.expressionAttributeValues, filter.expressionAttributeValues);
            }
        }

        // Add projection expression
        if (projection) {
            body.projectionExpression = projection.projectionExpression;
            Object.assign(body.expressionAttributeNames, projection.expressionAttributeNames);
        }

        if (append && state.queryLastKey) {
            body.lastKey = state.queryLastKey;
        }
        
        console.log('Query body:', JSON.stringify(body, null, 2));
        
        setQueryLoading(true);
        try {
            const path = selectedIndex
                ? `/tables/${state.currentTable}/gsi/${selectedIndex}/query`
                : `/tables/${state.currentTable}/query`;
            const data = await api.post(path, body);
            if (append) {
                renderQueryResults([...state.queryItems, ...data.items], data.lastKey);
            } else {
                renderQueryResults(data.items, data.lastKey);
            }
        } catch (err) {
            console.error('Query failed:', err);
            alert('Query failed: ' + err.message);
        } finally {
            setQueryLoading(false);
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

    // Run query in "Any" mode - raw PK/SK expression (like advanced query)
    async function runAnyQuery(append = false) {
        const indexName = $('#any-query-index')?.value || '';
        const pk = $('#any-query-pk')?.value;
        const skOp = $('#any-query-sk-op')?.value;
        const skVal = $('#any-query-sk-val')?.value;
        const skVal2 = $('#any-query-sk-val2')?.value;
        
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
        let expr = '#pk = :pk';
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
        
        // Sync projection state
        const projInput = $('#query-projection');
        if (projInput) state.queryProjection = projInput.value;

        const limit = parseInt($('#query-limit').value);
        const filter = buildQueryFilter();
        const projection = buildQueryProjection();

        const body = {
            keyConditionExpression: expr,
            expressionAttributeNames: names,
            expressionAttributeValues: values,
            limit: limit
        };

        // Add filter expression
        if (filter) {
            body.filterExpression = filter.filterExpression;
            Object.assign(body.expressionAttributeNames, filter.expressionAttributeNames);
            if (filter.expressionAttributeValues) {
                Object.assign(body.expressionAttributeValues, filter.expressionAttributeValues);
            }
        }

        // Add projection expression
        if (projection) {
            body.projectionExpression = projection.projectionExpression;
            Object.assign(body.expressionAttributeNames, projection.expressionAttributeNames);
        }

        if (append && state.queryLastKey) {
            body.lastKey = state.queryLastKey;
        }
        
        setQueryLoading(true);
        try {
            const path = indexName
                ? `/tables/${state.currentTable}/gsi/${indexName}/query`
                : `/tables/${state.currentTable}/query`;
            const data = await api.post(path, body);
            if (append) {
                renderQueryResults([...state.queryItems, ...data.items], data.lastKey);
            } else {
                renderQueryResults(data.items, data.lastKey);
            }
        } catch (err) {
            console.error('Query failed:', err);
            alert('Query failed: ' + err.message);
        } finally {
            setQueryLoading(false);
        }
    }

    // Show/hide scan spinner
    function setScanLoading(loading) {
        const play = $('.scan-play');
        const spinner = $('.scan-spinner');
        const btn = $('#btn-run-scan');
        if (play) play.style.display = loading ? 'none' : '';
        if (spinner) spinner.style.display = loading ? '' : 'none';
        if (btn) btn.disabled = loading;
    }

    // Show/hide query spinner
    function setQueryLoading(loading) {
        const play = $('.query-play');
        const spinner = $('.query-spinner');
        const btn = $('#btn-run-query');
        if (play) play.style.display = loading ? 'none' : '';
        if (spinner) spinner.style.display = loading ? '' : 'none';
        if (btn) btn.disabled = loading;
    }

    // Load data (scan)
    async function loadData(append = false) {
        if (state.loading) return;
        
        // Sync projection state from input
        const projInput = $('#scan-projection');
        if (projInput) state.scanProjection = projInput.value;

        const limit = parseInt($('#page-limit').value);
        const filter = buildScanFilter();
        const projection = buildScanProjection();

        state.loading = true;
        setScanLoading(true);
        try {
            let data;

            if (filter || projection) {
                // Use POST scan endpoint when filter or projection is active
                const body = { limit };

                if (filter) {
                    body.filterExpression = filter.filterExpression;
                    body.expressionAttributeValues = filter.expressionAttributeValues;
                }

                if (projection) {
                    body.projectionExpression = projection.projectionExpression;
                }

                // Merge expressionAttributeNames from filter and projection
                const mergedNames = {
                    ...(filter?.expressionAttributeNames || {}),
                    ...(projection?.expressionAttributeNames || {}),
                };
                if (Object.keys(mergedNames).length > 0) {
                    body.expressionAttributeNames = mergedNames;
                }

                if (append && state.lastKey) {
                    body.lastKey = state.lastKey;
                }
                data = await api.post(`/tables/${state.currentTable}/scan`, body);
            } else {
                // Use GET scan endpoint (no filter, no projection)
                const params = new URLSearchParams({ limit });
                if (append && state.lastKey) {
                    params.set('lastKey', state.lastKey);
                }
                data = await api.get(`/tables/${state.currentTable}/items?${params}`);
            }
            
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
            setScanLoading(false);
        }
    }

    // Render data table
    function renderData() {
        const schema = state.currentSchema;
        const pkName = schema.table.partitionKey.name;
        const skName = schema.table.sortKey?.name;

        // Pre-compute entity types for all items
        const allItemsWithEntity = state.items.map((item, originalIdx) => ({
            item,
            entityType: detectEntityType(item, schema),
            originalIdx
        }));

        // Apply client-side entity filter
        const entityFilter = state.scanEntityFilter;
        const hasEntityFilter = entityFilter.size > 0;
        const itemsWithEntity = hasEntityFilter
            ? allItemsWithEntity.filter(e => entityFilter.has(e.entityType))
            : allItemsWithEntity;

        // Get all unique keys from displayed items
        const allKeys = new Set();
        itemsWithEntity.forEach(({ item }) => {
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

        // Add Entity as first column, checkbox as zeroth
        const columns = ['_select', '_entity', ...sortedKeys];
        
        // Check if all displayed items are selected
        const allSelected = itemsWithEntity.length > 0 && itemsWithEntity.every(e => state.selectedIndices.has(e.originalIdx));

        // Collect distinct entity types from loaded items for the filter popover
        const distinctEntityTypes = [...new Set(allItemsWithEntity.map(e => e.entityType).filter(Boolean))].sort();

        // Render header
        const filterActive = hasEntityFilter ? ' active' : '';
        $('#data-thead').innerHTML = `
            <tr>
                <th class="checkbox-cell">
                    <input type="checkbox" id="select-all" ${allSelected ? 'checked' : ''} title="Select all">
                </th>
                <th class="th-entity">
                    <span>Entity</span>
                    <button class="th-filter-btn${filterActive}" id="entity-filter-btn" title="Filter by entity type">
                        <svg width="12" height="12" viewBox="0 0 16 16" fill="currentColor"><path d="M1 2a1 1 0 0 1 1-1h12a1 1 0 0 1 1 1v1.5a1 1 0 0 1-.293.707L10 8.914V13a1 1 0 0 1-.553.894l-2 1A1 1 0 0 1 6 14v-5.086L1.293 4.207A1 1 0 0 1 1 3.5V2z"/></svg>
                    </button>
                    <div class="th-filter-popover" id="entity-filter-popover" style="display:none">
                        <label class="th-filter-option">
                            <input type="checkbox" id="entity-filter-all" ${!hasEntityFilter ? 'checked' : ''}> <span>All</span>
                        </label>
                        <div class="th-filter-divider"></div>
                        ${distinctEntityTypes.map(et => `
                            <label class="th-filter-option">
                                <input type="checkbox" class="entity-filter-cb" value="${escapeHtml(et)}" ${(!hasEntityFilter || entityFilter.has(et)) ? 'checked' : ''}> <span class="entity-badge" data-entity="${escapeHtml(et)}" style="margin:0">${escapeHtml(et)}</span>
                            </label>
                        `).join('')}
                    </div>
                </th>
                ${sortedKeys.map(k => `<th>${k}</th>`).join('')}
            </tr>
        `;

        // Render rows
        if (itemsWithEntity.length === 0) {
            $('#data-tbody').innerHTML = `
                <tr>
                    <td colspan="${columns.length}" class="empty-state">
                        ${hasEntityFilter ? 'No items match the entity filter' : 'No items found'}
                    </td>
                </tr>
            `;
        } else {
            $('#data-tbody').innerHTML = itemsWithEntity.map(({ item, entityType, originalIdx }) => {
                const isSelected = state.selectedIndices.has(originalIdx);
                return `
                <tr data-index="${originalIdx}" class="${isSelected ? 'selected' : ''}">
                    <td class="checkbox-cell" onclick="event.stopPropagation()">
                        <input type="checkbox" class="row-select" data-index="${originalIdx}" ${isSelected ? 'checked' : ''}>
                    </td>
                    <td><span class="entity-badge" data-entity="${entityType || 'unknown'}">${entityType || '?'}</span></td>
                    ${sortedKeys.map(k => {
                        const val = item[k];
                        const isKey = k === pkName || k === skName;
                        const display = formatValue(val);
                        return `<td class="${isKey ? 'key-cell' : ''}" title="${escapeHtml(JSON.stringify(val))}">${display}</td>`;
                    }).join('')}
                </tr>
            `}).join('');
        }

        // Update count and load more button
        const countText = hasEntityFilter
            ? `${itemsWithEntity.length} of ${state.items.length} items (filtered)`
            : `${state.items.length} items`;
        $('#item-count').textContent = countText;
        $('#btn-load-more').style.display = state.lastKey ? 'inline-block' : 'none';
        
        // Update selection UI
        updateSelectionUI();
    }
    
    // Update selection UI (bulk actions bar, count, etc.)
    function updateSelectionUI() {
        const bar = $('#bulk-actions-bar');
        const count = state.selectedIndices.size;
        
        if (count > 0) {
            bar.style.display = 'flex';
            $('#selection-count').textContent = `${count} selected`;
        } else {
            bar.style.display = 'none';
        }
    }
    
    // Get the indices of currently visible (filtered) items
    function getVisibleIndices() {
        const entityFilter = state.scanEntityFilter;
        if (entityFilter.size === 0) {
            return state.items.map((_, idx) => idx);
        }
        const schema = state.currentSchema;
        return state.items
            .map((item, idx) => ({ idx, entityType: detectEntityType(item, schema) }))
            .filter(e => entityFilter.has(e.entityType))
            .map(e => e.idx);
    }

    // Toggle selection of a single row
    function toggleRowSelection(idx, selected) {
        if (selected) {
            state.selectedIndices.add(idx);
        } else {
            state.selectedIndices.delete(idx);
        }
        
        // Update row class
        const row = $(`tr[data-index="${idx}"]`);
        if (row) {
            row.classList.toggle('selected', selected);
        }
        
        // Update select-all checkbox state
        const selectAll = $('#select-all');
        if (selectAll) {
            const visibleIndices = getVisibleIndices();
            const allSelected = visibleIndices.length > 0 && visibleIndices.every(i => state.selectedIndices.has(i));
            selectAll.checked = allSelected;
            selectAll.indeterminate = state.selectedIndices.size > 0 && !allSelected;
        }
        
        updateSelectionUI();
    }
    
    // Select/deselect all (only visible/filtered items)
    function toggleSelectAll(selected) {
        state.selectedIndices.clear();
        if (selected) {
            getVisibleIndices().forEach(idx => state.selectedIndices.add(idx));
        }
        renderData();
    }
    
    // Clear selection
    function clearSelection() {
        state.selectedIndices.clear();
        renderData();
    }
    
    // Bulk delete selected items
    async function bulkDeleteSelected() {
        if (state.selectedIndices.size === 0) return;
        
        const count = state.selectedIndices.size;
        if (!confirmAWSWrite(`delete ${count} item(s)`)) {
            return;
        }
        
        // Additional non-AWS confirmation (keep original behavior)
        if (state.serverInfo?.mode !== 'aws' && !confirm(`Are you sure you want to delete ${count} item(s)?`)) {
            return;
        }
        
        const schema = state.currentSchema;
        const pkName = schema.table.partitionKey.name;
        const skName = schema.table.sortKey?.name;
        
        // Collect keys to delete
        const keys = [];
        for (const idx of state.selectedIndices) {
            const item = state.items[idx];
            const key = { pk: String(item[pkName]) };
            if (skName && item[skName] !== undefined) {
                key.sk = String(item[skName]);
            }
            keys.push(key);
        }
        
        try {
            await api.post(`/tables/${state.currentTable}/items/bulk-delete`, { keys });
            state.selectedIndices.clear();
            await loadData();
        } catch (err) {
            alert('Bulk delete failed: ' + err.message);
        }
    }

    // Convert items to CSV string
    function itemsToCSV(items) {
        if (items.length === 0) return '';
        
        // Get all unique keys from items
        const allKeys = new Set();
        items.forEach(item => {
            Object.keys(item).forEach(k => allKeys.add(k));
        });
        const columns = Array.from(allKeys).sort();
        
        // Helper to escape CSV values
        const escapeCSV = (val) => {
            if (val === null || val === undefined) return '';
            const str = typeof val === 'object' ? JSON.stringify(val) : String(val);
            // Escape quotes and wrap in quotes if contains comma, quote, or newline
            if (str.includes(',') || str.includes('"') || str.includes('\n')) {
                return '"' + str.replace(/"/g, '""') + '"';
            }
            return str;
        };
        
        // Build CSV
        const header = columns.map(escapeCSV).join(',');
        const rows = items.map(item => 
            columns.map(col => escapeCSV(item[col])).join(',')
        );
        
        return [header, ...rows].join('\n');
    }

    // Download CSV file
    function downloadCSV(csv, filename) {
        const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement('a');
        link.href = URL.createObjectURL(blob);
        link.download = filename;
        link.click();
        URL.revokeObjectURL(link.href);
    }

    // Export selected items to CSV (scan view)
    function exportSelectedToCSV() {
        if (state.selectedIndices.size === 0) return;
        
        const selectedItems = [];
        for (const idx of state.selectedIndices) {
            selectedItems.push(state.items[idx]);
        }
        
        const csv = itemsToCSV(selectedItems);
        const timestamp = new Date().toISOString().slice(0, 19).replace(/[:-]/g, '');
        downloadCSV(csv, `${state.currentTable}_export_${timestamp}.csv`);
    }

    // Export selected query items to CSV
    function exportQuerySelectedToCSV() {
        if (state.selectedQueryIndices.size === 0) return;
        
        const selectedItems = [];
        for (const idx of state.selectedQueryIndices) {
            selectedItems.push(state.queryItems[idx]);
        }
        
        const csv = itemsToCSV(selectedItems);
        const timestamp = new Date().toISOString().slice(0, 19).replace(/[:-]/g, '');
        downloadCSV(csv, `${state.currentTable}_query_export_${timestamp}.csv`);
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

        if (!confirmAWSWrite('save an item')) return;

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

        if (!confirmAWSWrite('delete an item')) {
            return;
        }

        // Additional non-AWS confirmation (keep original behavior)
        if (state.serverInfo?.mode !== 'aws' && !confirm('Are you sure you want to delete this item?')) {
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

    // Render query results using table view (similar to scan)
    function renderQueryResults(items, lastKey) {
        state.queryItems = items;
        state.queryLastKey = lastKey || null;
        state.selectedQueryIndices.clear();
        
        const schema = state.currentSchema;
        const pkName = schema.table.partitionKey.name;
        const skName = schema.table.sortKey?.name;

        // Pre-compute entity types for all items
        const itemsWithEntity = items.map(item => ({
            item,
            entityType: detectEntityType(item, schema)
        }));

        // Get all unique keys from items
        const allKeys = new Set();
        items.forEach(item => {
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

        // Check if all items are selected
        const allSelected = items.length > 0 && state.selectedQueryIndices.size === items.length;

        // Render header
        $('#query-data-thead').innerHTML = `
            <tr>
                <th class="checkbox-cell">
                    <input type="checkbox" id="query-select-all" ${allSelected ? 'checked' : ''} title="Select all">
                </th>
                ${['_entity', ...sortedKeys].map(k => `<th>${k === '_entity' ? 'Entity' : k}</th>`).join('')}
            </tr>
        `;

        // Render rows
        if (items.length === 0) {
            $('#query-data-tbody').innerHTML = `
                <tr>
                    <td colspan="${sortedKeys.length + 2}" class="empty-state">
                        No items found
                    </td>
                </tr>
            `;
        } else {
            $('#query-data-tbody').innerHTML = itemsWithEntity.map(({ item, entityType }, idx) => {
                const isSelected = state.selectedQueryIndices.has(idx);
                return `
                <tr data-query-index="${idx}" class="${isSelected ? 'selected' : ''}">
                    <td class="checkbox-cell" onclick="event.stopPropagation()">
                        <input type="checkbox" class="query-row-select" data-index="${idx}" ${isSelected ? 'checked' : ''}>
                    </td>
                    <td><span class="entity-badge" data-entity="${entityType || 'unknown'}">${entityType || '?'}</span></td>
                    ${sortedKeys.map(k => {
                        const val = item[k];
                        const isKey = k === pkName || k === skName;
                        const display = formatValue(val);
                        return `<td class="${isKey ? 'key-cell' : ''}" title="${escapeHtml(JSON.stringify(val))}">${display}</td>`;
                    }).join('')}
                </tr>
            `}).join('');
        }

        // Update count and load more button
        $('#query-item-count').textContent = `${items.length} item${items.length !== 1 ? 's' : ''}`;
        $('#btn-query-load-more').style.display = state.queryLastKey ? 'inline-block' : 'none';
        
        // Update selection UI
        updateQuerySelectionUI();
    }
    
    // Update query selection UI (bulk actions bar, count, etc.)
    function updateQuerySelectionUI() {
        const bar = $('#query-bulk-actions-bar');
        const count = state.selectedQueryIndices.size;
        
        if (count > 0) {
            bar.style.display = 'flex';
            $('#query-selection-count').textContent = `${count} selected`;
        } else {
            bar.style.display = 'none';
        }
    }
    
    // Toggle selection of a single query row
    function toggleQueryRowSelection(idx, selected) {
        if (selected) {
            state.selectedQueryIndices.add(idx);
        } else {
            state.selectedQueryIndices.delete(idx);
        }
        
        // Update row class
        const row = $(`tr[data-query-index="${idx}"]`);
        if (row) {
            row.classList.toggle('selected', selected);
        }
        
        // Update select-all checkbox state
        const selectAll = $('#query-select-all');
        if (selectAll) {
            const allSelected = state.queryItems.length > 0 && state.selectedQueryIndices.size === state.queryItems.length;
            selectAll.checked = allSelected;
            selectAll.indeterminate = state.selectedQueryIndices.size > 0 && !allSelected;
        }
        
        updateQuerySelectionUI();
    }
    
    // Select/deselect all query rows
    function toggleQuerySelectAll(selected) {
        state.selectedQueryIndices.clear();
        if (selected) {
            state.queryItems.forEach((_, idx) => state.selectedQueryIndices.add(idx));
        }
        renderQueryResults(state.queryItems, state.queryLastKey);
    }
    
    // Clear query selection
    function clearQuerySelection() {
        state.selectedQueryIndices.clear();
        renderQueryResults(state.queryItems, state.queryLastKey);
    }
    
    // Bulk delete selected query items
    async function bulkDeleteQuerySelected() {
        if (state.selectedQueryIndices.size === 0) return;
        
        const count = state.selectedQueryIndices.size;
        if (!confirmAWSWrite(`delete ${count} item(s)`)) {
            return;
        }
        
        // Additional non-AWS confirmation (keep original behavior)
        if (state.serverInfo?.mode !== 'aws' && !confirm(`Are you sure you want to delete ${count} item(s)?`)) {
            return;
        }
        
        const schema = state.currentSchema;
        const pkName = schema.table.partitionKey.name;
        const skName = schema.table.sortKey?.name;
        
        // Collect keys to delete
        const keys = [];
        for (const idx of state.selectedQueryIndices) {
            const item = state.queryItems[idx];
            const key = { pk: String(item[pkName]) };
            if (skName && item[skName] !== undefined) {
                key.sk = String(item[skName]);
            }
            keys.push(key);
        }
        
        try {
            await api.post(`/tables/${state.currentTable}/items/bulk-delete`, { keys });
            // Remove deleted items from queryItems and re-render
            const remainingItems = state.queryItems.filter((_, idx) => !state.selectedQueryIndices.has(idx));
            state.selectedQueryIndices.clear();
            renderQueryResults(remainingItems, state.queryLastKey);
        } catch (err) {
            alert('Bulk delete failed: ' + err.message);
        }
    }

    // --- Scan Filter Builder ---

    const FILTER_CONDITIONS = [
        { value: '=',                    label: 'Equal to' },
        { value: '<>',                   label: 'Not equal to' },
        { value: '<',                    label: 'Less than' },
        { value: '<=',                   label: 'Less than or equal to' },
        { value: '>',                    label: 'Greater than' },
        { value: '>=',                   label: 'Greater than or equal to' },
        { value: 'between',             label: 'Between' },
        { value: 'begins_with',         label: 'Begins with' },
        { value: 'contains',            label: 'Contains' },
        { value: 'attribute_exists',    label: 'Attribute exists' },
        { value: 'attribute_not_exists', label: 'Attribute not exists' },
    ];

    const FILTER_TYPES = [
        { value: 'S', label: 'String' },
        { value: 'N', label: 'Number' },
        { value: 'BOOL', label: 'Boolean' },
    ];

    function conditionNeedsValue(condition) {
        return condition !== 'attribute_exists' && condition !== 'attribute_not_exists';
    }

    function conditionNeedsSecondValue(condition) {
        return condition === 'between';
    }

    function addFilterRow() {
        state.scanFilterRows.push({
            attribute: '',
            condition: '=',
            type: 'S',
            value: '',
            value2: '',
        });
        renderFilterRows();
    }

    function removeFilterRow(idx) {
        state.scanFilterRows.splice(idx, 1);
        renderFilterRows();
        updateFilterCount();
    }

    function renderFilterRows() {
        const container = $('#scan-filter-rows');
        if (!container) return;

        if (state.scanFilterRows.length === 0) {
            container.innerHTML = '<div class="scan-filter-empty">No filters. Click "Add filter" to add one.</div>';
            return;
        }

        container.innerHTML = state.scanFilterRows.map((row, idx) => {
            const needsValue = conditionNeedsValue(row.condition);
            const needsValue2 = conditionNeedsSecondValue(row.condition);
            return `
            <div class="scan-filter-row" data-filter-idx="${idx}">
                <div class="scan-filter-field">
                    <label>Attribute name</label>
                    <input type="text" class="filter-attr" data-idx="${idx}" value="${escapeHtml(row.attribute)}" placeholder="e.g. status">
                </div>
                <div class="scan-filter-field">
                    <label>Condition</label>
                    <select class="filter-condition" data-idx="${idx}">
                        ${FILTER_CONDITIONS.map(c => `<option value="${c.value}"${c.value === row.condition ? ' selected' : ''}>${c.label}</option>`).join('')}
                    </select>
                </div>
                ${needsValue ? `
                <div class="scan-filter-field">
                    <label>Type</label>
                    <select class="filter-type" data-idx="${idx}">
                        ${FILTER_TYPES.map(t => `<option value="${t.value}"${t.value === row.type ? ' selected' : ''}>${t.label}</option>`).join('')}
                    </select>
                </div>
                <div class="scan-filter-field scan-filter-value-field">
                    <label>Value</label>
                    <input type="text" class="filter-value" data-idx="${idx}" value="${escapeHtml(row.value)}" placeholder="Enter value">
                </div>
                ` : ''}
                ${needsValue2 ? `
                <div class="scan-filter-field scan-filter-value-field">
                    <label>Value 2</label>
                    <input type="text" class="filter-value2" data-idx="${idx}" value="${escapeHtml(row.value2)}" placeholder="End value">
                </div>
                ` : ''}
                <div class="scan-filter-field scan-filter-remove">
                    <label>&nbsp;</label>
                    <button class="btn btn-danger btn-sm filter-remove-btn" data-idx="${idx}">Remove</button>
                </div>
            </div>`;
        }).join('');
    }

    function updateFilterCount() {
        const countEl = $('#scan-filter-count');
        if (!countEl) return;
        const n = state.scanFilterRows.length;
        countEl.textContent = n > 0 ? `(${n})` : '';
    }

    function syncFilterRowState() {
        // Read current values from DOM inputs back into state
        state.scanFilterRows.forEach((row, idx) => {
            const attr = $(`.filter-attr[data-idx="${idx}"]`);
            const cond = $(`.filter-condition[data-idx="${idx}"]`);
            const type = $(`.filter-type[data-idx="${idx}"]`);
            const val = $(`.filter-value[data-idx="${idx}"]`);
            const val2 = $(`.filter-value2[data-idx="${idx}"]`);
            if (attr) row.attribute = attr.value;
            if (cond) row.condition = cond.value;
            if (type) row.type = type.value;
            if (val) row.value = val.value;
            if (val2) row.value2 = val2.value;
        });
    }

    // Convert a typed value to JSON-compatible value for the API
    function convertFilterValue(rawValue, type) {
        switch (type) {
            case 'N':
                return parseFloat(rawValue);
            case 'BOOL':
                return rawValue.toLowerCase() === 'true';
            default:
                return rawValue;
        }
    }

    // Build filterExpression, expressionAttributeNames, expressionAttributeValues from filter rows
    function buildScanFilter() {
        syncFilterRowState();
        const rows = state.scanFilterRows.filter(r => r.attribute.trim() !== '');
        if (rows.length === 0) return null;

        const expressions = [];
        const names = {};
        const values = {};

        rows.forEach((row, idx) => {
            const nameKey = `#fattr${idx}`;
            const valKey = `:fval${idx}`;
            const valKey2 = `:fval${idx}b`;
            names[nameKey] = row.attribute.trim();

            const needsVal = conditionNeedsValue(row.condition);
            if (needsVal) {
                values[valKey] = convertFilterValue(row.value, row.type);
            }

            switch (row.condition) {
                case '=':
                case '<>':
                case '<':
                case '<=':
                case '>':
                case '>=':
                    expressions.push(`${nameKey} ${row.condition} ${valKey}`);
                    break;
                case 'between':
                    values[valKey2] = convertFilterValue(row.value2, row.type);
                    expressions.push(`${nameKey} BETWEEN ${valKey} AND ${valKey2}`);
                    break;
                case 'begins_with':
                    expressions.push(`begins_with(${nameKey}, ${valKey})`);
                    break;
                case 'contains':
                    expressions.push(`contains(${nameKey}, ${valKey})`);
                    break;
                case 'attribute_exists':
                    expressions.push(`attribute_exists(${nameKey})`);
                    break;
                case 'attribute_not_exists':
                    expressions.push(`attribute_not_exists(${nameKey})`);
                    break;
            }
        });

        if (expressions.length === 0) return null;

        return {
            filterExpression: expressions.join(' AND '),
            expressionAttributeNames: names,
            expressionAttributeValues: Object.keys(values).length > 0 ? values : undefined,
        };
    }

    // Build projectionExpression and its expressionAttributeNames from the projection input
    function buildScanProjection() {
        const input = $('#scan-projection');
        const raw = (input ? input.value : state.scanProjection).trim();
        if (!raw) return null;

        // Split on commas, trim each attribute path
        const attrs = raw.split(',').map(a => a.trim()).filter(a => a);
        if (attrs.length === 0) return null;

        const names = {};
        const projParts = [];

        attrs.forEach((attr, idx) => {
            // Handle nested paths like "info.rating" -> "#pattr0.#pattr1"
            const segments = attr.split('.');
            const aliased = segments.map((seg, segIdx) => {
                const nameKey = `#pattr${idx}_${segIdx}`;
                names[nameKey] = seg;
                return nameKey;
            });
            projParts.push(aliased.join('.'));
        });

        return {
            projectionExpression: projParts.join(', '),
            expressionAttributeNames: names,
        };
    }

    function resetFilters() {
        state.scanFilterRows = [];
        state.scanProjection = '';
        state.scanEntityFilter = new Set();
        const projInput = $('#scan-projection');
        if (projInput) projInput.value = '';
        renderFilterRows();
        updateFilterCount();
        state.selectedIndices.clear();
        loadData();
    }

    // --- End Scan Filter Builder ---

    // --- Query Filter Builder ---

    function addQueryFilterRow() {
        state.queryFilterRows.push({
            attribute: '',
            condition: '=',
            type: 'S',
            value: '',
            value2: '',
        });
        renderQueryFilterRows();
    }

    function removeQueryFilterRow(idx) {
        state.queryFilterRows.splice(idx, 1);
        renderQueryFilterRows();
        updateQueryFilterCount();
    }

    function renderQueryFilterRows() {
        const container = $('#query-filter-rows');
        if (!container) return;

        if (state.queryFilterRows.length === 0) {
            container.innerHTML = '<div class="scan-filter-empty">No filters. Click "Add filter" to add one.</div>';
            return;
        }

        container.innerHTML = state.queryFilterRows.map((row, idx) => {
            const needsValue = conditionNeedsValue(row.condition);
            const needsValue2 = conditionNeedsSecondValue(row.condition);
            return `
            <div class="scan-filter-row" data-qfilter-idx="${idx}">
                <div class="scan-filter-field">
                    <label>Attribute name</label>
                    <input type="text" class="qfilter-attr" data-idx="${idx}" value="${escapeHtml(row.attribute)}" placeholder="e.g. status">
                </div>
                <div class="scan-filter-field">
                    <label>Condition</label>
                    <select class="qfilter-condition" data-idx="${idx}">
                        ${FILTER_CONDITIONS.map(c => `<option value="${c.value}"${c.value === row.condition ? ' selected' : ''}>${c.label}</option>`).join('')}
                    </select>
                </div>
                ${needsValue ? `
                <div class="scan-filter-field">
                    <label>Type</label>
                    <select class="qfilter-type" data-idx="${idx}">
                        ${FILTER_TYPES.map(t => `<option value="${t.value}"${t.value === row.type ? ' selected' : ''}>${t.label}</option>`).join('')}
                    </select>
                </div>
                <div class="scan-filter-field scan-filter-value-field">
                    <label>Value</label>
                    <input type="text" class="qfilter-value" data-idx="${idx}" value="${escapeHtml(row.value)}" placeholder="Enter value">
                </div>
                ` : ''}
                ${needsValue2 ? `
                <div class="scan-filter-field scan-filter-value-field">
                    <label>Value 2</label>
                    <input type="text" class="qfilter-value2" data-idx="${idx}" value="${escapeHtml(row.value2)}" placeholder="End value">
                </div>
                ` : ''}
                <div class="scan-filter-field scan-filter-remove">
                    <label>&nbsp;</label>
                    <button class="btn btn-danger btn-sm qfilter-remove-btn" data-idx="${idx}">Remove</button>
                </div>
            </div>`;
        }).join('');
    }

    function updateQueryFilterCount() {
        const countEl = $('#query-filter-count');
        if (!countEl) return;
        const n = state.queryFilterRows.length;
        countEl.textContent = n > 0 ? `(${n})` : '';
    }

    function syncQueryFilterRowState() {
        state.queryFilterRows.forEach((row, idx) => {
            const attr = $(`.qfilter-attr[data-idx="${idx}"]`);
            const cond = $(`.qfilter-condition[data-idx="${idx}"]`);
            const type = $(`.qfilter-type[data-idx="${idx}"]`);
            const val = $(`.qfilter-value[data-idx="${idx}"]`);
            const val2 = $(`.qfilter-value2[data-idx="${idx}"]`);
            if (attr) row.attribute = attr.value;
            if (cond) row.condition = cond.value;
            if (type) row.type = type.value;
            if (val) row.value = val.value;
            if (val2) row.value2 = val2.value;
        });
    }

    function buildQueryFilter() {
        syncQueryFilterRowState();
        const rows = state.queryFilterRows.filter(r => r.attribute.trim() !== '');
        if (rows.length === 0) return null;

        const expressions = [];
        const names = {};
        const values = {};

        rows.forEach((row, idx) => {
            const nameKey = `#qfattr${idx}`;
            const valKey = `:qfval${idx}`;
            const valKey2 = `:qfval${idx}b`;
            names[nameKey] = row.attribute.trim();

            const needsVal = conditionNeedsValue(row.condition);
            if (needsVal) {
                values[valKey] = convertFilterValue(row.value, row.type);
            }

            switch (row.condition) {
                case '=':
                case '<>':
                case '<':
                case '<=':
                case '>':
                case '>=':
                    expressions.push(`${nameKey} ${row.condition} ${valKey}`);
                    break;
                case 'between':
                    values[valKey2] = convertFilterValue(row.value2, row.type);
                    expressions.push(`${nameKey} BETWEEN ${valKey} AND ${valKey2}`);
                    break;
                case 'begins_with':
                    expressions.push(`begins_with(${nameKey}, ${valKey})`);
                    break;
                case 'contains':
                    expressions.push(`contains(${nameKey}, ${valKey})`);
                    break;
                case 'attribute_exists':
                    expressions.push(`attribute_exists(${nameKey})`);
                    break;
                case 'attribute_not_exists':
                    expressions.push(`attribute_not_exists(${nameKey})`);
                    break;
            }
        });

        if (expressions.length === 0) return null;

        return {
            filterExpression: expressions.join(' AND '),
            expressionAttributeNames: names,
            expressionAttributeValues: Object.keys(values).length > 0 ? values : undefined,
        };
    }

    function buildQueryProjection() {
        const input = $('#query-projection');
        const raw = (input ? input.value : state.queryProjection).trim();
        if (!raw) return null;

        const attrs = raw.split(',').map(a => a.trim()).filter(a => a);
        if (attrs.length === 0) return null;

        const names = {};
        const projParts = [];

        attrs.forEach((attr, idx) => {
            const segments = attr.split('.');
            const aliased = segments.map((seg, segIdx) => {
                const nameKey = `#qpattr${idx}_${segIdx}`;
                names[nameKey] = seg;
                return nameKey;
            });
            projParts.push(aliased.join('.'));
        });

        return {
            projectionExpression: projParts.join(', '),
            expressionAttributeNames: names,
        };
    }

    function resetQueryFilters() {
        state.queryFilterRows = [];
        state.queryProjection = '';
        const projInput = $('#query-projection');
        if (projInput) projInput.value = '';
        renderQueryFilterRows();
        updateQueryFilterCount();
    }

    // --- End Query Filter Builder ---

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
                const clickedTable = tableLink.dataset.table;
                // If already on query tab for this table, reset to "Any" mode
                const queryTabActive = $('.tab[data-tab="query"]')?.classList.contains('active');
                if (queryTabActive && state.currentTable === clickedTable) {
                    // Reset entity dropdown to "Any"
                    const entitySelect = $('#query-entity');
                    if (entitySelect) {
                        entitySelect.value = '__any__';
                        renderEntityQueryFields('__any__');
                    }
                    // Update sidebar highlighting
                    $$('.table-list .entity-link').forEach(a => a.classList.remove('active'));
                    $$('.table-list .table-link').forEach(a => a.classList.remove('active'));
                    tableLink.classList.add('active');
                    // Update breadcrumb
                    $('#table-name').textContent = clickedTable;
                    return;
                }
                selectTable(clickedTable);
            }
        });

        // Tabs
        $$('.tab').forEach(tab => {
            tab.addEventListener('click', async () => {
                const tabName = tab.dataset.tab;
                $$('.tab').forEach(t => t.classList.remove('active'));
                $$('.tab-content').forEach(c => c.classList.remove('active'));
                tab.classList.add('active');
                $(`#tab-${tabName}`).classList.add('active');
                
                // Auto-load data when switching to scan tab if not loaded yet
                if (tabName === 'data' && state.currentTable && state.items.length === 0) {
                    await loadData();
                }
            });
        });

        // Refresh button
        $('#btn-refresh').addEventListener('click', () => {
            state.selectedIndices.clear();
            loadData();
        });

        // Run scan button (play icon in toolbar)
        $('#btn-run-scan').addEventListener('click', () => {
            state.selectedIndices.clear();
            updateFilterCount();
            loadData();
        });

        // New item button
        $('#btn-new-item').addEventListener('click', () => openItemModal(null, true));

        // Page limit change
        $('#page-limit').addEventListener('change', () => {
            state.selectedIndices.clear();
            loadData();
        });

        // Entity filter popover (delegated - elements are inside dynamic thead)
        document.addEventListener('click', (e) => {
            const btn = e.target.closest('#entity-filter-btn');
            const popover = document.getElementById('entity-filter-popover');
            if (btn && popover) {
                e.stopPropagation();
                popover.style.display = popover.style.display === 'none' ? 'block' : 'none';
                return;
            }
            // Clicking inside popover
            if (e.target.closest('#entity-filter-popover')) {
                return; // handled by change listener below
            }
            // Click outside - close any open popover
            if (popover && popover.style.display !== 'none') {
                popover.style.display = 'none';
            }
        });

        // Entity filter checkbox changes (delegated)
        document.addEventListener('change', (e) => {
            const popover = document.getElementById('entity-filter-popover');
            if (!popover || !popover.contains(e.target)) return;

            if (e.target.id === 'entity-filter-all') {
                // "All" checkbox toggled
                if (e.target.checked) {
                    state.scanEntityFilter = new Set();
                    // Check all individual checkboxes
                    popover.querySelectorAll('.entity-filter-cb').forEach(cb => cb.checked = true);
                } else {
                    // Uncheck all - show nothing (will show empty state)
                    const allTypes = [...popover.querySelectorAll('.entity-filter-cb')].map(cb => cb.value);
                    state.scanEntityFilter = new Set(); // empty = all, but we want none
                    popover.querySelectorAll('.entity-filter-cb').forEach(cb => cb.checked = false);
                    // We need a way to distinguish "all" from "none" — unchecking All re-checks it and keeps showing all
                    e.target.checked = true;
                    return;
                }
            } else if (e.target.classList.contains('entity-filter-cb')) {
                // Individual entity checkbox toggled
                const checked = [...popover.querySelectorAll('.entity-filter-cb')]
                    .filter(cb => cb.checked)
                    .map(cb => cb.value);
                const allCbs = popover.querySelectorAll('.entity-filter-cb');
                const allCheckbox = document.getElementById('entity-filter-all');

                if (checked.length === allCbs.length || checked.length === 0) {
                    // All checked or none checked → show all
                    state.scanEntityFilter = new Set();
                    if (allCheckbox) allCheckbox.checked = true;
                    allCbs.forEach(cb => cb.checked = true);
                } else {
                    state.scanEntityFilter = new Set(checked);
                    if (allCheckbox) allCheckbox.checked = false;
                }
            }

            state.selectedIndices.clear();
            renderData();
        });

        // Scan filter toggle (expand/collapse)
        $('#scan-filters-toggle').addEventListener('click', () => {
            const body = $('#scan-filters-body');
            const arrow = $('#scan-filters-toggle .toggle-arrow');
            const isHidden = body.style.display === 'none';
            body.style.display = isHidden ? 'block' : 'none';
            arrow.textContent = isHidden ? '\u25BC' : '\u25B6';
        });

        // Query filter toggle (expand/collapse)
        $('#query-filters-toggle').addEventListener('click', () => {
            const body = $('#query-filters-body');
            const arrow = $('#query-filters-toggle .toggle-arrow');
            const isHidden = body.style.display === 'none';
            body.style.display = isHidden ? 'block' : 'none';
            arrow.textContent = isHidden ? '\u25BC' : '\u25B6';
        });

        // Add filter row
        $('#btn-add-filter').addEventListener('click', () => {
            addFilterRow();
            updateFilterCount();
        });

        // Apply filters (run scan with filter)
        $('#btn-apply-filters').addEventListener('click', () => {
            state.selectedIndices.clear();
            updateFilterCount();
            loadData();
        });

        // Reset filters
        $('#btn-reset-filters').addEventListener('click', resetFilters);

        // Filter row changes (delegated)
        $('#scan-filter-rows').addEventListener('change', (e) => {
            const idx = parseInt(e.target.dataset.idx);
            if (isNaN(idx)) return;

            if (e.target.classList.contains('filter-condition')) {
                // Condition changed - need to re-render row (may show/hide value inputs)
                syncFilterRowState();
                state.scanFilterRows[idx].condition = e.target.value;
                renderFilterRows();
            } else if (e.target.classList.contains('filter-attr')) {
                state.scanFilterRows[idx].attribute = e.target.value;
            } else if (e.target.classList.contains('filter-type')) {
                state.scanFilterRows[idx].type = e.target.value;
            }
        });

        $('#scan-filter-rows').addEventListener('input', (e) => {
            const idx = parseInt(e.target.dataset.idx);
            if (isNaN(idx)) return;
            if (e.target.classList.contains('filter-value')) {
                state.scanFilterRows[idx].value = e.target.value;
            } else if (e.target.classList.contains('filter-value2')) {
                state.scanFilterRows[idx].value2 = e.target.value;
            } else if (e.target.classList.contains('filter-attr')) {
                state.scanFilterRows[idx].attribute = e.target.value;
            }
        });

        // Remove filter row (delegated)
        $('#scan-filter-rows').addEventListener('click', (e) => {
            if (e.target.classList.contains('filter-remove-btn')) {
                const idx = parseInt(e.target.dataset.idx);
                removeFilterRow(idx);
            }
        });

        // --- Query filter event listeners ---

        // Run query button (play icon in toolbar)
        $('#btn-run-query').addEventListener('click', () => {
            state.selectedQueryIndices.clear();
            updateQueryFilterCount();
            runEntityQuery();
        });

        // Query limit change
        // (no auto-run on limit change for query - user must click play)

        // Add query filter row
        $('#btn-query-add-filter').addEventListener('click', () => {
            addQueryFilterRow();
            updateQueryFilterCount();
        });

        // Apply query filters (run query with filter)
        $('#btn-query-apply-filters').addEventListener('click', () => {
            state.selectedQueryIndices.clear();
            updateQueryFilterCount();
            runEntityQuery();
        });

        // Reset query filters
        $('#btn-query-reset-filters').addEventListener('click', resetQueryFilters);

        // Query filter row changes (delegated)
        $('#query-filter-rows').addEventListener('change', (e) => {
            const idx = parseInt(e.target.dataset.idx);
            if (isNaN(idx)) return;

            if (e.target.classList.contains('qfilter-condition')) {
                syncQueryFilterRowState();
                state.queryFilterRows[idx].condition = e.target.value;
                renderQueryFilterRows();
            } else if (e.target.classList.contains('qfilter-attr')) {
                state.queryFilterRows[idx].attribute = e.target.value;
            } else if (e.target.classList.contains('qfilter-type')) {
                state.queryFilterRows[idx].type = e.target.value;
            }
        });

        $('#query-filter-rows').addEventListener('input', (e) => {
            const idx = parseInt(e.target.dataset.idx);
            if (isNaN(idx)) return;
            if (e.target.classList.contains('qfilter-value')) {
                state.queryFilterRows[idx].value = e.target.value;
            } else if (e.target.classList.contains('qfilter-value2')) {
                state.queryFilterRows[idx].value2 = e.target.value;
            } else if (e.target.classList.contains('qfilter-attr')) {
                state.queryFilterRows[idx].attribute = e.target.value;
            }
        });

        // Remove query filter row (delegated)
        $('#query-filter-rows').addEventListener('click', (e) => {
            if (e.target.classList.contains('qfilter-remove-btn')) {
                const idx = parseInt(e.target.dataset.idx);
                removeQueryFilterRow(idx);
            }
        });

        // Query load more button
        $('#btn-query-load-more').addEventListener('click', () => runEntityQuery(true));

        // Load more button
        $('#btn-load-more').addEventListener('click', () => loadData(true));

        // Row click to edit (excluding checkbox cell)
        $('#data-tbody').addEventListener('click', (e) => {
            // Don't trigger edit if clicking checkbox
            if (e.target.classList.contains('row-select') || e.target.closest('.checkbox-cell')) {
                return;
            }
            const row = e.target.closest('tr[data-index]');
            if (row) {
                const idx = parseInt(row.dataset.index);
                openItemModal(state.items[idx]);
            }
        });
        
        // Query results row click to edit (excluding checkbox cell)
        $('#query-data-tbody').addEventListener('click', (e) => {
            // Don't trigger edit if clicking checkbox
            if (e.target.classList.contains('query-row-select') || e.target.closest('.checkbox-cell')) {
                return;
            }
            const row = e.target.closest('tr[data-query-index]');
            if (row) {
                const idx = parseInt(row.dataset.queryIndex);
                openItemModal(state.queryItems[idx]);
            }
        });
        
        // Query row checkbox selection
        $('#query-data-tbody').addEventListener('change', (e) => {
            if (e.target.classList.contains('query-row-select')) {
                const idx = parseInt(e.target.dataset.index);
                toggleQueryRowSelection(idx, e.target.checked);
            }
        });
        
        // Query select all checkbox
        $('#query-data-thead').addEventListener('change', (e) => {
            if (e.target.id === 'query-select-all') {
                toggleQuerySelectAll(e.target.checked);
            }
        });
        
        // Query bulk delete button
        $('#btn-query-bulk-delete').addEventListener('click', bulkDeleteQuerySelected);
        
        // Query export CSV button
        $('#btn-query-export-csv').addEventListener('click', exportQuerySelectedToCSV);
        
        // Query clear selection button
        $('#btn-query-clear-selection').addEventListener('click', clearQuerySelection);
        
        // Row checkbox selection
        $('#data-tbody').addEventListener('change', (e) => {
            if (e.target.classList.contains('row-select')) {
                const idx = parseInt(e.target.dataset.index);
                toggleRowSelection(idx, e.target.checked);
            }
        });
        
        // Select all checkbox
        $('#data-thead').addEventListener('change', (e) => {
            if (e.target.id === 'select-all') {
                toggleSelectAll(e.target.checked);
            }
        });
        
        // Bulk delete button
        $('#btn-bulk-delete').addEventListener('click', bulkDeleteSelected);
        
        // Export CSV button
        $('#btn-export-csv').addEventListener('click', exportSelectedToCSV);
        
        // Clear selection button
        $('#btn-clear-selection').addEventListener('click', clearSelection);

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
        
        // Query entity selection - update sidebar and breadcrumbs too
        document.addEventListener('change', (e) => {
            if (e.target.id === 'query-entity') {
                const entityType = e.target.value;
                state.selectedQueryIndex = '';  // reset index when entity changes
                renderEntityQueryFields(entityType);
                
                // Update sidebar highlighting
                $$('.table-list .entity-link').forEach(a => a.classList.remove('active'));
                $$('.table-list .table-link').forEach(a => a.classList.remove('active'));
                if (entityType && entityType !== '__any__' && state.currentTable) {
                    const entityLink = $(`.entity-link[data-table="${state.currentTable}"][data-entity="${entityType}"]`);
                    if (entityLink) entityLink.classList.add('active');
                    // Update breadcrumb
                    $('#table-name').textContent = `${state.currentTable} / ${entityType}`;
                } else if (state.currentTable) {
                    // "Any" or no selection - highlight table, show table name only
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

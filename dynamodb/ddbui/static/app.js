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
        selectedQueryIndex: ''  // currently selected index for entity query ('' = primary)
    };

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
                state.selectedIndices.clear();
                state.selectedQueryIndex = '';
            }

            // Update UI
            $('#table-name').textContent = entity ? `${tableName} / ${entity}` : tableName;
            $('#welcome-view').classList.remove('active');
            $('#table-view').classList.add('active');

            // Render schema
            renderSchema();
            
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
        
        html += `<button id="btn-entity-query" class="btn btn-primary">Run Query</button>`;
        
        container.innerHTML = html;
        
        // Attach index change listener
        attachIndexChangeListener(entityType);
        
        // Re-attach event listener for the button
        const btn = $('#btn-entity-query');
        if (btn) {
            btn.addEventListener('click', runEntityQuery);
        }
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
        
        html += `<button id="btn-entity-query" class="btn btn-primary">Run Query</button>`;
        
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
        
        // "Any" mode - run raw query
        if (entityType === '__any__') {
            await runAnyQuery();
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
        
        const body = {
            keyConditionExpression: expr,
            expressionAttributeNames: names,
            expressionAttributeValues: values,
            limit: 50
        };
        
        console.log('Query body:', JSON.stringify(body, null, 2));
        
        try {
            const path = selectedIndex
                ? `/tables/${state.currentTable}/gsi/${selectedIndex}/query`
                : `/tables/${state.currentTable}/query`;
            const data = await api.post(path, body);
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

    // Run query in "Any" mode - raw PK/SK expression (like advanced query)
    async function runAnyQuery() {
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

        // Add Entity as first column, checkbox as zeroth
        const columns = ['_select', '_entity', ...sortedKeys];
        
        // Check if all items are selected
        const allSelected = state.items.length > 0 && state.selectedIndices.size === state.items.length;

        // Render header
        $('#data-thead').innerHTML = `
            <tr>
                <th class="checkbox-cell">
                    <input type="checkbox" id="select-all" ${allSelected ? 'checked' : ''} title="Select all">
                </th>
                ${['_entity', ...sortedKeys].map(k => `<th>${k === '_entity' ? 'Entity' : k}</th>`).join('')}
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
            $('#data-tbody').innerHTML = itemsWithEntity.map(({ item, entityType }, idx) => {
                const isSelected = state.selectedIndices.has(idx);
                return `
                <tr data-index="${idx}" class="${isSelected ? 'selected' : ''}">
                    <td class="checkbox-cell" onclick="event.stopPropagation()">
                        <input type="checkbox" class="row-select" data-index="${idx}" ${isSelected ? 'checked' : ''}>
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
        $('#item-count').textContent = `${state.items.length} items`;
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
            const allSelected = state.items.length > 0 && state.selectedIndices.size === state.items.length;
            selectAll.checked = allSelected;
            selectAll.indeterminate = state.selectedIndices.size > 0 && !allSelected;
        }
        
        updateSelectionUI();
    }
    
    // Select/deselect all
    function toggleSelectAll(selected) {
        state.selectedIndices.clear();
        if (selected) {
            state.items.forEach((_, idx) => state.selectedIndices.add(idx));
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
        if (!confirm(`Are you sure you want to delete ${count} item(s)?`)) {
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

    // Render query results using table view (similar to scan)
    function renderQueryResults(items) {
        state.queryItems = items;
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

        // Update count
        $('#query-item-count').textContent = `(${items.length} item${items.length !== 1 ? 's' : ''})`;
        
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
        renderQueryResults(state.queryItems);
    }
    
    // Clear query selection
    function clearQuerySelection() {
        state.selectedQueryIndices.clear();
        renderQueryResults(state.queryItems);
    }
    
    // Bulk delete selected query items
    async function bulkDeleteQuerySelected() {
        if (state.selectedQueryIndices.size === 0) return;
        
        const count = state.selectedQueryIndices.size;
        if (!confirm(`Are you sure you want to delete ${count} item(s)?`)) {
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
            renderQueryResults(remainingItems);
        } catch (err) {
            alert('Bulk delete failed: ' + err.message);
        }
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

        // New item button
        $('#btn-new-item').addEventListener('click', () => openItemModal(null, true));

        // Page limit change
        $('#page-limit').addEventListener('change', () => {
            state.selectedIndices.clear();
            loadData();
        });

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

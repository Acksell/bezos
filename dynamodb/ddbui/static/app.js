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
            renderTableList();
        } catch (err) {
            console.error('Failed to load tables:', err);
        }
    }

    // Render table list in sidebar
    function renderTableList() {
        const list = $('#table-list');
        list.innerHTML = state.tables.map(t => `
            <li>
                <a href="#" data-table="${t.name}">
                    ${t.name}
                    <span class="badge">${t.entityCount} entities</span>
                </a>
            </li>
        `).join('');
    }

    // Select a table
    async function selectTable(tableName) {
        // Update active state
        $$('.table-list a').forEach(a => a.classList.remove('active'));
        $(`.table-list a[data-table="${tableName}"]`).classList.add('active');

        // Load table schema
        try {
            state.currentTable = tableName;
            state.currentSchema = await api.get(`/tables/${tableName}`);
            state.items = [];
            state.lastKey = null;

            // Update UI
            $('#table-name').textContent = tableName;
            $('#welcome-view').classList.remove('active');
            $('#table-view').classList.add('active');

            // Render schema
            renderSchema();

            // Populate query index dropdown
            populateIndexDropdown();

            // Load data
            await loadData();
        } catch (err) {
            console.error('Failed to load table:', err);
            alert('Failed to load table: ' + err.message);
        }
    }

    // Render schema tab
    function renderSchema() {
        const schema = state.currentSchema;

        // Table structure
        let tableHtml = `
            <div class="schema-card">
                <div class="schema-row">
                    <span class="schema-label">Partition Key:</span>
                    <span class="schema-value">${schema.table.partitionKey.name} (${schema.table.partitionKey.kind})</span>
                </div>
        `;
        if (schema.table.sortKey) {
            tableHtml += `
                <div class="schema-row">
                    <span class="schema-label">Sort Key:</span>
                    <span class="schema-value">${schema.table.sortKey.name} (${schema.table.sortKey.kind})</span>
                </div>
            `;
        }
        tableHtml += '</div>';
        $('#schema-table').innerHTML = tableHtml;

        // GSIs
        if (schema.table.gsis && schema.table.gsis.length > 0) {
            $('#schema-gsis').innerHTML = schema.table.gsis.map(gsi => `
                <div class="schema-card">
                    <h4>${gsi.name}</h4>
                    <div class="schema-row">
                        <span class="schema-label">Partition Key:</span>
                        <span class="schema-value">${gsi.partitionKey.name} (${gsi.partitionKey.kind})</span>
                    </div>
                    ${gsi.sortKey ? `
                    <div class="schema-row">
                        <span class="schema-label">Sort Key:</span>
                        <span class="schema-value">${gsi.sortKey.name} (${gsi.sortKey.kind})</span>
                    </div>` : ''}
                </div>
            `).join('');
        } else {
            $('#schema-gsis').innerHTML = '<p class="placeholder">No GSIs defined</p>';
        }

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

        // Render header
        $('#data-thead').innerHTML = `
            <tr>
                ${sortedKeys.map(k => `<th>${k}</th>`).join('')}
            </tr>
        `;

        // Render rows
        if (state.items.length === 0) {
            $('#data-tbody').innerHTML = `
                <tr>
                    <td colspan="${sortedKeys.length}" class="empty-state">
                        No items found
                    </td>
                </tr>
            `;
        } else {
            $('#data-tbody').innerHTML = state.items.map((item, idx) => `
                <tr data-index="${idx}">
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
            const link = e.target.closest('a[data-table]');
            if (link) {
                e.preventDefault();
                selectTable(link.dataset.table);
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

        // Sort key operator change
        $('#query-sk-op').addEventListener('change', (e) => {
            const val2 = $('#query-sk-val2');
            val2.style.display = e.target.value === 'between' ? 'block' : 'none';
        });

        // Run query
        $('#btn-run-query').addEventListener('click', runQuery);

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

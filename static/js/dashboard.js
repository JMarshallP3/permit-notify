// Modern Dashboard JavaScript
class PermitDashboard {
    constructor() {
        this.permits = [];
        this.filteredPermits = [];
        this.filters = {
            operator: '',
            county: '',
            purpose: '',
            queue: ''
        };
        this.sortBy = 'status_date';
        this.sortOrder = 'desc';
        this.lastUpdate = null;
        this.autoRefresh = true;
        this.refreshInterval = 300000; // 5 minutes (further reduced for better performance)
        this.dismissedPermits = new Set(); // Track dismissed permits
        this.showDismissed = false; // Show dismissed permits toggle
        this.todayOnly = false; // Show today only toggle
        this.selectedOperators = new Set(); // Selected operators for multi-select
        this.selectedCounties = new Set(); // Selected counties for multi-select
        
            // Reservoir mapping - this will learn new reservoirs
            this.reservoirMapping = {
                // Based on your examples
                'HAWKVILLE (AUSTIN CHALK)': 'AUSTIN CHALK',
                'SPRABERRY (TREND AREA)': 'SPRABERRY', 
                'PHANTOM (WOLFCAMP)': 'WOLFCAMP',
                'SUGARKANE (EAGLE FORD)': 'EAGLE FORD',
                'EMMA (BARNETT SHALE)': 'BARNETT SHALE',
                // Add more mappings as needed
                'EAGLE FORD': 'EAGLE FORD',
                'WOLFCAMP': 'WOLFCAMP',
                'AUSTIN CHALK': 'AUSTIN CHALK',
                'BARNETT SHALE': 'BARNETT SHALE'
            };
            
            // Review queue for deferred reservoir mappings
            this.reviewQueue = [];
            
            // Cancelled mappings that user can review later
            this.cancelledMappings = [];
        
        this.init();
        
        // Initialize dark mode
        setTimeout(() => this.initializeDarkMode(), 100);
    }
    
    init() {
        this.loadDismissedPermits();
        this.loadReviewQueue();
        this.loadCancelledMappings();
        this.setupEventListeners();
        this.loadPermits();
        this.startAutoRefresh();
        this.updateStats();
        
        // Ensure no reservoir management content appears on main dashboard
        this.cleanupMainDashboard();
    }
    
    // Prevent reservoir management content from appearing on main dashboard
    cleanupMainDashboard() {
        // Remove any reservoir management content that might have been inserted
        const mainContainer = document.getElementById('permitsContainer');
        if (mainContainer) {
            // Remove any elements that contain reservoir management content
            const reservoirElements = mainContainer.querySelectorAll('[data-tab="review"], [data-tab="saved"], .reservoir-tab, .reservoir-management-modal');
            reservoirElements.forEach(el => el.remove());
        }
        
        // More aggressive cleanup - check entire document for reservoir management content
        const reservoirSections = document.querySelectorAll('.reservoir-management, .reservoir-tabs, .reservoir-content, .reservoir-tab-content, .reservoir-management-modal');
        reservoirSections.forEach(el => {
            // Only remove if not inside a modal
            if (!el.closest('.reservoir-manager-modal')) {
                console.log('Removing reservoir management content from main dashboard:', el);
                el.remove();
            }
        });
        
        // Check for elements with text content that indicates reservoir management
        // BUT be more selective to avoid removing essential DOM elements
        const suspiciousDivs = document.querySelectorAll('div:not(.container):not(.dashboard-grid):not(.main-content):not(.card):not(.permit-card)');
        suspiciousDivs.forEach(div => {
            const text = div.textContent;
            if (text && (
                text.includes('Under Review') || 
                text.includes('Saved Mappings') ||
                text.includes('B.L BUSH') ||
                text.includes('BUSH 23') ||
                text.includes('BIVINS')
            )) {
                // Only remove if not inside a modal, not in sidebar, and not essential structure
                if (!div.closest('.reservoir-manager-modal') && 
                    !div.closest('.sidebar') && 
                    !div.closest('.card-title') &&
                    !div.closest('.container') &&
                    !div.closest('.dashboard-grid') &&
                    !div.closest('.main-content')) {
                    console.log('Removing div with reservoir management text:', div);
                    div.remove();
                }
            }
        });
        
        // Set up a mutation observer to prevent future insertions
        this.setupMainDashboardProtection();
    }
    
    // Set up protection against reservoir management content being inserted into main dashboard
    setupMainDashboardProtection() {
        const mainContainer = document.getElementById('permitsContainer');
        if (!mainContainer) return;
        
        const observer = new MutationObserver((mutations) => {
            mutations.forEach((mutation) => {
                mutation.addedNodes.forEach((node) => {
                    if (node.nodeType === Node.ELEMENT_NODE) {
                        // Check if added node contains reservoir management content
                        if (node.matches && (
                            node.matches('.reservoir-management, .reservoir-tabs, .reservoir-content, .reservoir-tab-content') ||
                            node.querySelector('.reservoir-management, .reservoir-tabs, .reservoir-content, .reservoir-tab-content')
                        )) {
                            // Only remove if not inside a modal
                            if (!node.closest('.reservoir-manager-modal')) {
                                console.warn('Removing reservoir management content from main dashboard');
                                node.remove();
                            }
                        }
                        
                        // Also check text content - but be very selective
                        const text = node.textContent;
                        if (text && (
                            text.includes('B.L BUSH') ||
                            text.includes('BUSH 23') ||
                            text.includes('BIVINS')
                        ) && text.length < 200) { // Only short text snippets, not entire page content
                            // Only remove if not inside a modal, sidebar, or essential structure
                            if (!node.closest('.reservoir-manager-modal') && 
                                !node.closest('.sidebar') && 
                                !node.closest('.card-title') &&
                                !node.closest('.container') &&
                                !node.closest('.dashboard-grid') &&
                                !node.closest('.main-content') &&
                                !node.matches('.container, .dashboard-grid, .main-content, .card, .permit-card')) {
                                console.warn('Removing node with reservoir management text from main dashboard');
                                node.remove();
                            }
                        }
                    }
                });
            });
        });
        
        observer.observe(mainContainer, { childList: true, subtree: true });
        
        // Also observe the entire document body for more comprehensive protection
        const bodyObserver = new MutationObserver((mutations) => {
            mutations.forEach((mutation) => {
                mutation.addedNodes.forEach((node) => {
                    if (node.nodeType === Node.ELEMENT_NODE && 
                        !node.closest('.reservoir-manager-modal') && 
                        !node.closest('.sidebar')) {
                        
                        // Check for reservoir management classes
                        if (node.matches && (
                            node.matches('.reservoir-management, .reservoir-tabs, .reservoir-content, .reservoir-tab-content') ||
                            node.querySelector('.reservoir-management, .reservoir-tabs, .reservoir-content, .reservoir-tab-content')
                        )) {
                            console.warn('Removing reservoir management content from document body');
                            node.remove();
                        }
                    }
                });
            });
        });
        
        bodyObserver.observe(document.body, { childList: true, subtree: true });
    }
    
    setupEventListeners() {
        // Filter inputs
        document.getElementById('operatorFilter').addEventListener('input', (e) => {
            this.filters.operator = e.target.value.toLowerCase();
            this.applyFilters();
        });
        
        document.getElementById('countyFilter').addEventListener('input', (e) => {
            this.filters.county = e.target.value.toLowerCase();
            this.applyFilters();
        });
        
        document.getElementById('purposeFilter').addEventListener('change', (e) => {
            this.filters.purpose = e.target.value.toLowerCase();
            this.applyFilters();
        });
        
        document.getElementById('queueFilter').addEventListener('change', (e) => {
            this.filters.queue = e.target.value.toLowerCase();
            this.applyFilters();
        });
        
        // Refresh button
        document.getElementById('refreshBtn').addEventListener('click', () => {
            this.loadPermits();
        });
        
        // Auto-refresh toggle
        document.getElementById('autoRefreshToggle').addEventListener('change', (e) => {
            this.autoRefresh = e.target.checked;
            if (this.autoRefresh) {
                this.startAutoRefresh();
            } else {
                this.stopAutoRefresh();
            }
        });
        
        // Clear filters
        document.getElementById('clearFilters').addEventListener('click', () => {
            this.clearFilters();
        });
        
        // Show dismissed toggle
        document.getElementById('showDismissedToggle').addEventListener('change', (e) => {
            this.showDismissed = e.target.checked;
            this.applyFilters();
        });
        
        // Today only toggle
        document.getElementById('todayOnlyToggle').addEventListener('change', (e) => {
            this.todayOnly = e.target.checked;
            this.applyFilters();
        });
        
        // Date range selector
        const daysBackSelect = document.getElementById('daysBackSelect');
        if (daysBackSelect) {
            this.daysBack = 7; // Default to 7 days
            daysBackSelect.addEventListener('change', (e) => {
                this.daysBack = parseInt(e.target.value);
                this.loadPermits(); // Reload with new date range
            });
        }
        
        // Multi-select dropdown toggles
        document.getElementById('operatorMultiBtn').addEventListener('click', () => {
            this.toggleMultiSelect('operatorMultiSelect');
        });
        
        document.getElementById('countyMultiBtn').addEventListener('click', () => {
            this.toggleMultiSelect('countyMultiSelect');
        });
    }
    
    async loadPermits() {
        try {
            this.showLoading(true);
            
            const daysBack = this.daysBack || 7;
            // Add cache-busting parameter to force fresh data
            const cacheBuster = Date.now();
            const response = await fetch(`/api/v1/permits?limit=100&days_back=${daysBack}&_cb=${cacheBuster}`, {
                cache: 'no-cache',
                headers: {
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache'
                }
            });
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            const data = await response.json();
            this.permits = data.permits || [];
            this.lastUpdate = new Date();
            
            // Debug: Log county data to console
            console.log('🏛️ County data check:', this.permits.map(p => ({ 
                status_no: p.status_no, 
                county: p.county, 
                operator: p.operator_name?.substring(0, 30) 
            })));
            
            this.applyFilters();
            this.updateStats();
            this.updateLastRefreshTime();
            this.buildMultiSelectOptions();
            
            // Cleanup any reservoir management content that might have appeared
            // Disabled automatic cleanup to prevent removing essential DOM elements
            // setTimeout(() => this.cleanupMainDashboard(), 100);
            
        } catch (error) {
            console.error('Error loading permits:', error);
            this.showError('Failed to load permits. Please try again.');
        } finally {
            this.showLoading(false);
        }
    }
    
    applyFilters() {
        console.log('applyFilters called - showDismissed:', this.showDismissed);
        console.log('dismissedPermits:', [...this.dismissedPermits]);
        
        this.filteredPermits = this.permits.filter(permit => {
            const operator = (permit.operator_name || '').toLowerCase();
            const county = (permit.county || '').toLowerCase();
            const purpose = (permit.filing_purpose || '').toLowerCase();
            const queue = (permit.current_queue || '').toLowerCase();
            
            // Today only filter
            const isToday = this.todayOnly ? this.isPermitFromToday(permit) : true;
            
            // Multi-select operator filter
            const operatorMatch = this.selectedOperators.size === 0 || 
                this.selectedOperators.has(this.cleanOperatorName(permit.operator_name));
            
            // Multi-select county filter  
            const countyMatch = this.selectedCounties.size === 0 || 
                this.selectedCounties.has(permit.county);
            
            // Check if this permit should be filtered out due to dismiss
            const isDismissed = this.dismissedPermits.has(permit.status_no);
            const shouldShowDismissed = this.showDismissed || !isDismissed;
            
            if (isDismissed && !this.showDismissed) {
                console.log(`Filtering out dismissed permit: ${permit.status_no}`);
            }
            
            return (
                (!this.filters.operator || operator.includes(this.filters.operator)) &&
                (!this.filters.county || county.includes(this.filters.county)) &&
                (!this.filters.purpose || purpose.includes(this.filters.purpose)) &&
                (!this.filters.queue || queue.includes(this.filters.queue)) &&
                shouldShowDismissed &&
                isToday &&
                operatorMatch &&
                countyMatch
            );
        });
        
        this.sortPermits();
        this.renderPermitCards();
        this.updateFilterStats();
    }
    
    isPermitFromToday(permit) {
        if (!permit.status_date) return false;
        
        // Handle different date formats from API
        let permitDate;
        const dateStr = permit.status_date;
        
        // Debug logging for date issues
        if (this.permits.length > 0 && Math.random() < 0.01) { // Log 1% of the time to avoid spam
            console.log('🗓️ Date debugging - permit.status_date:', dateStr, 'type:', typeof dateStr);
        }
        
        // Convert MM-DD-YYYY to ISO format (YYYY-MM-DD) for reliable parsing
        if (dateStr.includes('-') && dateStr.length === 10) {
            const parts = dateStr.split('-');
            if (parts.length === 3 && parts[0].length === 2) {
                // Assume MM-DD-YYYY format - use Date constructor with individual components to avoid timezone issues
                const [month, day, year] = parts;
                permitDate = new Date(parseInt(year), parseInt(month) - 1, parseInt(day)); // Month is 0-indexed in JS Date
            } else {
                permitDate = new Date(dateStr);
            }
        } else {
            permitDate = new Date(dateStr);
        }
        
        const today = new Date();
        
        // Debug logging for date comparison
        if (this.permits.length > 0 && Math.random() < 0.01) { // Log 1% of the time
            console.log('🗓️ Date comparison:', {
                permitDateStr: dateStr,
                permitDate: permitDate.toDateString(),
                today: today.toDateString(),
                isToday: (
                    permitDate.getFullYear() === today.getFullYear() &&
                    permitDate.getMonth() === today.getMonth() &&
                    permitDate.getDate() === today.getDate()
                )
            });
        }
        
        // Compare dates by year, month, and day (ignore time)
        return (
            permitDate.getFullYear() === today.getFullYear() &&
            permitDate.getMonth() === today.getMonth() &&
            permitDate.getDate() === today.getDate()
        );
    }
    
    sortPermits() {
        this.filteredPermits.sort((a, b) => {
            let aVal = a[this.sortBy] || '';
            let bVal = b[this.sortBy] || '';
            
            // Handle date sorting
            if (this.sortBy === 'status_date') {
                aVal = new Date(aVal || 0);
                bVal = new Date(bVal || 0);
            }
            
            if (this.sortOrder === 'asc') {
                return aVal > bVal ? 1 : -1;
            } else {
                return aVal < bVal ? 1 : -1;
            }
        });
    }
    
    renderPermitCards() {
        const container = document.getElementById('permitsContainer');
        
        if (this.filteredPermits.length === 0) {
            container.innerHTML = `
                <div style="text-align: center; padding: 3rem; color: var(--text-secondary);">
                    <h3>No permits found matching your filters</h3>
                    <p>Try adjusting your search criteria</p>
                </div>
            `;
            return;
        }
        
        // Group permits by county
        const permitsByCounty = {};
        this.filteredPermits.forEach(permit => {
            const county = permit.county || 'Unknown County';
            if (!permitsByCounty[county]) {
                permitsByCounty[county] = [];
            }
            permitsByCounty[county].push(permit);
        });
        
        // Sort counties alphabetically
        const sortedCounties = Object.keys(permitsByCounty).sort();
        
        container.innerHTML = sortedCounties.map(county => `
            <div class="county-section">
                <div class="county-header">
                    <div class="county-name">${county}</div>
                    <div class="county-count">${permitsByCounty[county].length} permit${permitsByCounty[county].length !== 1 ? 's' : ''}</div>
                </div>
                <div class="permits-grid">
                    ${permitsByCounty[county].map(permit => this.renderPermitCard(permit)).join('')}
                </div>
            </div>
        `).join('');
        
        // Add event listeners for dismiss buttons
        this.addCardEventListeners();
    }
    
    renderPermitCard(permit) {
        const isDismissed = this.dismissedPermits.has(permit.status_no);
        
        return `
            <div class="permit-card ${isDismissed ? 'dismissed' : ''}" data-permit-id="${permit.status_no}">
                <div class="permit-card-header">
                    <div class="permit-date">${this.formatDate(permit.created_at || permit.status_date)}</div>
                </div>
                
                <div class="permit-card-body">
                    <div class="permit-info-row" style="align-items: flex-start;">
                        <span class="permit-label" style="margin-top: 0;">Operator</span>
                        <span class="permit-value operator-name">${this.cleanOperatorName(permit.operator_name) || '-'}</span>
                    </div>
                    
                    <div class="permit-info-row">
                        <span class="permit-label">Lease</span>
                        <span class="permit-value lease-name">${permit.lease_name || '-'}</span>
                    </div>
                    
                    <div class="permit-info-row">
                        <span class="permit-label">Well #</span>
                        <span class="permit-value">${permit.well_no || '-'}</span>
                    </div>
                    
                    <div class="permit-info-row">
                        <span class="permit-label">Purpose</span>
                        <span class="permit-value">
                            <span class="permit-status ${this.getStatusClass(permit.filing_purpose)}">
                                ${permit.filing_purpose || '-'}
                            </span>
                        </span>
                    </div>
                    
                    <div class="permit-info-row">
                        <span class="permit-label">Queue</span>
                        <span class="permit-value">
                            <span class="permit-status ${this.getQueueClass(permit.current_queue)}">
                                ${permit.current_queue || '-'}
                            </span>
                        </span>
                    </div>
                    
                    ${permit.field_name ? `
                        <div class="permit-info-row">
                            <span class="permit-label">Reservoir</span>
                            <span class="permit-value">
                                <span class="reservoir-display">${this.extractReservoir(permit.field_name)}</span>
                            </span>
                        </div>
                    ` : ''}
                    
                    ${permit.acres ? `
                        <div class="permit-info-row">
                            <span class="permit-label">Acres</span>
                            <span class="permit-value">${permit.acres}</span>
                        </div>
                    ` : ''}
                </div>
                
                <div class="permit-card-actions">
                    ${permit.detail_url ? `
                        <a href="${permit.detail_url}" target="_blank" class="btn btn-sm btn-outline" style="text-decoration: none; display: inline-block;">
                            📄 View Permit
                        </a>
                    ` : ''}
                    
                    <button class="btn btn-sm btn-warning" onclick="window.dashboard.openManualMappingForPermit(${JSON.stringify(permit).replace(/"/g, '&quot;')})">
                        ⚙️ Manage Reservoir
                    </button>
                    
                    <button class="btn btn-sm btn-success" onclick="window.dashboard.flagForReenrich('${permit.status_no}')">
                        🔄 Re-enrich
                        </button>
                    
                    <button class="btn btn-sm btn-outline" data-permit-id="${permit.status_no}" onclick="window.dashboard.dismissPermit('${permit.status_no}')">
                        ✕ Dismiss
                    </button>
                </div>
            </div>
        `;
    }
    
    addCardEventListeners() {
        // Add dismiss button listeners
        document.querySelectorAll('.btn-dismiss').forEach(button => {
            button.addEventListener('click', (e) => {
                e.stopPropagation();
                const permitId = button.dataset.permitId;
                this.dismissPermit(permitId);
            });
        });
        
        // Add card click listeners for details
        document.querySelectorAll('.permit-card').forEach(card => {
            card.addEventListener('click', (e) => {
                if (e.target.tagName === 'A' || e.target.tagName === 'BUTTON') return; // Don't trigger on buttons/links
                
                const permitId = card.dataset.permitId;
                const permit = this.permits.find(p => p.status_no === permitId);
                if (permit) {
                    this.showPermitDetails(permit);
                }
            });
        });
    }
    
    async dismissPermit(permitId) {
        console.log('Dismissing permit:', permitId);
        console.log('showDismissed before:', this.showDismissed);
        console.log('dismissedPermits before:', [...this.dismissedPermits]);
        
        // Check if this is an injection well
        const permit = this.permits.find(p => p.status_no === permitId);
        const isInjectionWell = permit && this.isInjectionWell(permit);
        
        // If it's an injection well, flag it in the database
        if (isInjectionWell) {
            try {
                console.log('🚫 Flagging injection well in database:', permitId);
                const response = await fetch(`/api/v1/permits/${permitId}/flag-injection-well`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Cache-Control': 'no-cache'
                    }
                });
                
                if (response.ok) {
                    const result = await response.json();
                    console.log('✅ Successfully flagged injection well:', result);
                } else {
                    console.error('❌ Failed to flag injection well:', response.status, response.statusText);
                }
            } catch (error) {
                console.error('❌ Error flagging injection well:', error);
            }
        }
        
        this.dismissedPermits.add(permitId);
        
        console.log('dismissedPermits after:', [...this.dismissedPermits]);
        
        // Add visual feedback
        const card = document.querySelector(`[data-permit-id="${permitId}"]`);
        if (card) {
            card.classList.add('dismissed');
            
            // Remove after animation
            setTimeout(() => {
                console.log('Applying filters after dismiss...');
                this.applyFilters(); // Re-render without dismissed permit
            }, 300);
        }
        
        // Store dismissed permits in localStorage for persistence
        localStorage.setItem('dismissedPermits', JSON.stringify([...this.dismissedPermits]));
        
        this.updateFilterStats();
    }
    
    loadDismissedPermits() {
        const stored = localStorage.getItem('dismissedPermits');
        if (stored) {
            this.dismissedPermits = new Set(JSON.parse(stored));
        }
    }
    
    loadReviewQueue() {
        const reviewQueue = localStorage.getItem('reservoirReviewQueue');
        if (reviewQueue) {
            this.reviewQueue = JSON.parse(reviewQueue);
        }
        this.updateReviewQueueDisplay();
        
        // Load stored reservoir mappings
        const storedMappings = localStorage.getItem('reservoirMapping');
        if (storedMappings) {
            this.reservoirMapping = {...this.reservoirMapping, ...JSON.parse(storedMappings)};
        }
    }
    
    loadCancelledMappings() {
        const cancelledMappings = localStorage.getItem('cancelledReservoirMappings');
        if (cancelledMappings) {
            this.cancelledMappings = JSON.parse(cancelledMappings);
        }
    }
    
    addToCancelledMappings(fieldName, suggestedReservoir) {
        // Check if already in cancelled list
        const existingIndex = this.cancelledMappings.findIndex(item => item.fieldName === fieldName);
        
        if (existingIndex === -1) {
            this.cancelledMappings.push({
                fieldName: fieldName,
                suggestedReservoir: suggestedReservoir,
                cancelledAt: new Date().toISOString(),
                permits: this.getPermitsWithFieldName(fieldName)
            });
            
            localStorage.setItem('cancelledReservoirMappings', JSON.stringify(this.cancelledMappings));
        }
    }
    
    removeFromCancelledMappings(fieldName) {
        this.cancelledMappings = this.cancelledMappings.filter(item => item.fieldName !== fieldName);
        localStorage.setItem('cancelledReservoirMappings', JSON.stringify(this.cancelledMappings));
    }
    
    addToReviewQueue(fieldName, suggestedReservoir) {
        // Check if already in queue
        const existingIndex = this.reviewQueue.findIndex(item => item.fieldName === fieldName);
        
        if (existingIndex === -1) {
            this.reviewQueue.push({
                fieldName: fieldName,
                suggestedReservoir: suggestedReservoir,
                addedAt: new Date().toISOString(),
                permits: this.getPermitsWithFieldName(fieldName)
            });
            
            localStorage.setItem('reservoirReviewQueue', JSON.stringify(this.reviewQueue));
            this.updateReviewQueueDisplay();
        }
    }
    
    getPermitsWithFieldName(fieldName) {
        return this.permits
            .filter(permit => permit.field_name === fieldName)
            .map(permit => ({
                id: permit.id, // Include the permit ID
                status_no: permit.status_no,
                lease_name: permit.lease_name,
                county: permit.county,
                detail_url: permit.detail_url,
                status_date: permit.status_date // Also include status_date for display
            }))
            .slice(0, 3); // Limit to first 3 permits
    }
    
    // Reservoir Name Learning System - Integrated with Reservoir Management
    async correctReservoirName(wrongReservoirName, permits) {
        try {
            const correctReservoir = prompt(
                `🎯 RESERVOIR CORRECTION\n\n` +
                `Current Reservoir: "${wrongReservoirName}"\n` +
                `Affects ${permits.length} permit${permits.length !== 1 ? 's' : ''}\n\n` +
                `Enter the CORRECT geological reservoir name:`
            );
            
            if (!correctReservoir || correctReservoir.trim() === '') {
                return;
            }
            
            if (correctReservoir.trim() === wrongReservoirName) {
                alert('Reservoir name is already correct');
                return;
            }
            
            // Use the helper function to apply the correction
            await this.applyReservoirCorrection(wrongReservoirName, permits, correctReservoir.trim());
            
        } catch (error) {
            console.error('Reservoir correction error:', error);
            alert(`Error recording correction: ${error.message}`);
        }
    }
    
    // Helper function to get permit ID by status number
    getPermitIdByStatusNo(statusNo) {
        const permit = this.permits.find(p => p.status_no === statusNo);
        if (permit && permit.id) {
            return permit.id;
        }
        
        // If not found in main permits list, try to find in review queue permits
        for (const item of this.reviewQueue) {
            const reviewPermit = item.permits.find(p => p.status_no === statusNo);
            if (reviewPermit && reviewPermit.id) {
                return reviewPermit.id;
            }
        }
        
        console.warn(`Could not find permit ID for status ${statusNo}`);
        console.log('Available permits in main list:', this.permits.map(p => ({status: p.status_no, id: p.id})));
        console.log('Available permits in review queue:', this.reviewQueue.map(item => 
            item.permits.map(p => ({status: p.status_no, id: p.id}))
        ));
        return null;
    }
    
    // Helper function to apply reservoir correction (used by both manual and AI suggestion)
    async applyReservoirCorrection(wrongReservoirName, permits, correctReservoir) {
        // Show loading message
        const loadingMsg = document.createElement('div');
        loadingMsg.style.cssText = 'position: fixed; top: 20px; right: 20px; background: #3b82f6; color: white; padding: 12px 20px; border-radius: 8px; z-index: 1000; font-weight: 500;';
        loadingMsg.textContent = `🤖 Correcting reservoir for ${permits.length} permits...`;
        document.body.appendChild(loadingMsg);
        
        let successCount = 0;
        
        // Correct each permit
        for (const permit of permits) {
            try {
                const response = await fetch('/api/v1/field-corrections/correct', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        permit_id: permit.id || this.getPermitIdByStatusNo(permit.status_no),
                        status_no: permit.status_no,
                        wrong_field: wrongReservoirName,
                        correct_field: correctReservoir,
                        detail_url: permit.detail_url
                    })
                });
                
                if (response.ok) {
                    successCount++;
                }
            } catch (error) {
                console.error(`Error correcting permit ${permit.status_no}:`, error);
            }
        }
        
        document.body.removeChild(loadingMsg);
        
        if (successCount > 0) {
            // Show success message
            const successMsg = document.createElement('div');
            successMsg.style.cssText = 'position: fixed; top: 20px; right: 20px; background: #10b981; color: white; padding: 12px 20px; border-radius: 8px; z-index: 1000; font-weight: 500;';
            successMsg.textContent = `✅ Corrected ${successCount} permits! "${wrongReservoirName}" → "${correctReservoir}"`;
            document.body.appendChild(successMsg);
            
            // Remove from review queue and add to saved mappings
            this.removeFromReviewQueue(wrongReservoirName);
            this.addToSavedMappings(wrongReservoirName, correctReservoir);
            
            // Refresh tabs and permit data
            setTimeout(() => {
                document.body.removeChild(successMsg);
                this.loadPermitData();
                this.updateReviewQueueDisplay();
                this.updateSavedMappingsDisplay();
            }, 2000);
            
        } else {
            alert('Failed to correct any permits');
        }
    }
    
    // Helper function to add mapping to saved mappings
    addToSavedMappings(fieldName, reservoirName) {
        const savedMappings = JSON.parse(localStorage.getItem('savedMappings') || '[]');
        
        // Check if mapping already exists
        const existingMapping = savedMappings.find(mapping => 
            mapping.fieldName.toLowerCase() === fieldName.toLowerCase()
        );
        
        if (!existingMapping) {
            savedMappings.push({
                fieldName: fieldName,
                reservoirName: reservoirName,
                savedAt: new Date().toISOString(),
                source: 'correction' // Mark as coming from correction system
            });
            
            localStorage.setItem('savedMappings', JSON.stringify(savedMappings));
        }
    }
    
    // Accept a reservoir name as correct and move to saved mappings
    async acceptCorrectReservoir(currentFieldName, suggestedReservoir, statusNo) {
        try {
            // When accepting, we're saying the CURRENT field name is correct
            // So we save the current field name as the correct mapping
            this.addToSavedMappings(currentFieldName, currentFieldName);
            
            // Remove this specific permit from review queue
            this.removeSinglePermitFromReview(currentFieldName, statusNo);
            
            // Show success message
            const successMsg = document.createElement('div');
            successMsg.style.cssText = 'position: fixed; top: 20px; right: 20px; background: #10b981; color: white; padding: 12px 20px; border-radius: 8px; z-index: 1000; font-weight: 500;';
            successMsg.textContent = `✅ Accepted "${currentFieldName}" as correct for permit ${statusNo}`;
            document.body.appendChild(successMsg);
            
            // Refresh tabs
            setTimeout(() => {
                document.body.removeChild(successMsg);
                this.updateReviewQueueDisplay();
                // Update the saved mappings tab if it exists
                if (typeof this.updateSavedMappingsDisplay === 'function') {
                    this.updateSavedMappingsDisplay();
                } else {
                    // Stay in current tab (Under Review) after accepting
                    this.switchReservoirTab('review');
                }
            }, 2000);
            
        } catch (error) {
            console.error('Accept reservoir error:', error);
            alert(`Error accepting reservoir: ${error.message}`);
        }
    }
    
    // Correct a single permit's reservoir
    async correctSinglePermit(permit) {
        try {
            const correctReservoir = prompt(
                `🎯 CORRECT RESERVOIR\n\n` +
                `Permit: ${permit.status_no}\n` +
                `Lease: ${permit.lease_name || 'Unknown'}\n` +
                `Current Field: "${permit.currentFieldName}"\n\n` +
                `Enter the CORRECT geological reservoir name:`
            );
            
            if (!correctReservoir || correctReservoir.trim() === '') {
                return;
            }
            
            if (correctReservoir.trim() === permit.currentFieldName) {
                alert('Reservoir name is already correct');
                return;
            }
            
            // Apply correction to this permit
            await this.applySinglePermitCorrection(permit, correctReservoir.trim());
            
        } catch (error) {
            console.error('Single permit correction error:', error);
            alert(`Error correcting permit: ${error.message}`);
        }
    }
    
    // Apply correction to a single permit
    async applySinglePermitCorrection(permit, correctReservoir) {
        // Show loading message
        const loadingMsg = document.createElement('div');
        loadingMsg.style.cssText = 'position: fixed; top: 20px; right: 20px; background: #3b82f6; color: white; padding: 12px 20px; border-radius: 8px; z-index: 1000; font-weight: 500;';
        loadingMsg.textContent = `🤖 Correcting permit ${permit.status_no}...`;
        document.body.appendChild(loadingMsg);
        
        try {
            const permitId = permit.id || this.getPermitIdByStatusNo(permit.status_no);
            
            if (!permitId) {
                document.body.removeChild(loadingMsg);
                alert(`Cannot find permit ID for status ${permit.status_no}. Please refresh the page and try again.`);
                return;
            }
            
            const response = await fetch('/api/v1/field-corrections/correct', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    permit_id: permitId,
                    status_no: permit.status_no,
                    wrong_field: permit.currentFieldName,
                    correct_field: correctReservoir,
                    detail_url: permit.detail_url,
                    html_context: "" // Add empty html_context to satisfy API
                })
            });
            
            document.body.removeChild(loadingMsg);
            
            if (response.ok) {
                // Show success message
                const successMsg = document.createElement('div');
                successMsg.style.cssText = 'position: fixed; top: 20px; right: 20px; background: #10b981; color: white; padding: 12px 20px; border-radius: 8px; z-index: 1000; font-weight: 500;';
                successMsg.textContent = `✅ Corrected permit ${permit.status_no}: "${permit.currentFieldName}" → "${correctReservoir}"`;
                document.body.appendChild(successMsg);
                
                // Add to saved mappings and remove from review
                this.addToSavedMappings(permit.currentFieldName, correctReservoir);
                this.removeSinglePermitFromReview(permit.currentFieldName, permit.status_no);
                
                // Refresh tabs and permit data
                setTimeout(() => {
                    document.body.removeChild(successMsg);
                    this.loadPermits();
                    this.updateReviewQueueDisplay();
                    // Update the saved mappings tab if it exists
                    if (typeof this.updateSavedMappingsDisplay === 'function') {
                        this.updateSavedMappingsDisplay();
                    } else {
                        // Stay in current tab (Under Review) after correcting
                        this.switchReservoirTab('review');
                    }
                }, 2000);
                
            } else {
                const errorData = await response.json().catch(() => ({}));
                const errorMessage = errorData.detail || `HTTP ${response.status}: ${response.statusText}`;
                alert(`Failed to correct permit: ${errorMessage}`);
            }
            
        } catch (error) {
            document.body.removeChild(loadingMsg);
            throw error;
        }
    }
    
    // Get AI suggestion for a single permit
    async getSinglePermitSuggestion(permit) {
        try {
            const permitId = permit.id || this.getPermitIdByStatusNo(permit.status_no);
            
            if (!permitId) {
                alert('Unable to find permit ID for suggestion');
                return;
            }
            
            // Show loading message
            const loadingMsg = document.createElement('div');
            loadingMsg.style.cssText = 'position: fixed; top: 20px; right: 20px; background: #3b82f6; color: white; padding: 12px 20px; border-radius: 8px; z-index: 1000; font-weight: 500;';
            loadingMsg.textContent = '🤖 Getting AI suggestion...';
            document.body.appendChild(loadingMsg);
            
            const response = await fetch(`/api/v1/field-corrections/suggest/${permitId}`);
            
            document.body.removeChild(loadingMsg);
            
            if (response.ok) {
                const result = await response.json();
                
                if (result.has_suggestion) {
                    const useIt = confirm(
                        `🤖 AI RESERVOIR SUGGESTION\n\n` +
                        `Permit: ${permit.status_no}\n` +
                        `Lease: ${permit.lease_name || 'Unknown'}\n` +
                        `Current: "${permit.currentFieldName}"\n` +
                        `AI Suggests: "${result.suggested_field}"\n\n` +
                        `Apply this suggestion?`
                    );
                    
                    if (useIt) {
                        await this.applySinglePermitCorrection(permit, result.suggested_field);
                    }
                } else {
                    // Show info message
                    const infoMsg = document.createElement('div');
                    infoMsg.style.cssText = 'position: fixed; top: 20px; right: 20px; background: #f59e0b; color: white; padding: 12px 20px; border-radius: 8px; z-index: 1000; font-weight: 500;';
                    infoMsg.textContent = 'ℹ️ No AI suggestion available for this permit yet';
                    document.body.appendChild(infoMsg);
                    
                    setTimeout(() => {
                        document.body.removeChild(infoMsg);
                    }, 3000);
                }
                
            } else {
                // Show error message
                const errorMsg = document.createElement('div');
                errorMsg.style.cssText = 'position: fixed; top: 20px; right: 20px; background: #ef4444; color: white; padding: 12px 20px; border-radius: 8px; z-index: 1000; font-weight: 500;';
                errorMsg.textContent = '❌ Failed to get AI suggestion';
                document.body.appendChild(errorMsg);
                
                setTimeout(() => {
                    document.body.removeChild(errorMsg);
                }, 3000);
            }
            
        } catch (error) {
            console.error('AI suggestion error:', error);
            // Show error message
            const errorMsg = document.createElement('div');
            errorMsg.style.cssText = 'position: fixed; top: 20px; right: 20px; background: #ef4444; color: white; padding: 12px 20px; border-radius: 8px; z-index: 1000; font-weight: 500;';
            errorMsg.textContent = `❌ Error getting suggestion: ${error.message}`;
            document.body.appendChild(errorMsg);
            
            setTimeout(() => {
                document.body.removeChild(errorMsg);
            }, 3000);
        }
    }
    
    // Remove a single permit from review queue
    removeSinglePermitFromReview(fieldName, statusNo) {
        console.log(`Removing permit ${statusNo} with field name "${fieldName}" from review queue`);
        console.log('Current review queue:', this.reviewQueue);
        
        this.reviewQueue = this.reviewQueue.map(item => {
            if (item.fieldName === fieldName) {
                console.log(`Found matching field name. Current permits:`, item.permits);
                // Remove the specific permit from this item
                const updatedPermits = item.permits.filter(permit => permit.status_no !== statusNo);
                console.log(`After filtering, permits:`, updatedPermits);
                
                if (updatedPermits.length === 0) {
                    // If no permits left, mark for removal
                    console.log(`No permits left for field "${fieldName}", removing entire item`);
                    return null;
                } else {
                    // Return item with updated permits list
                    console.log(`${updatedPermits.length} permits remaining for field "${fieldName}"`);
                    return {
                        ...item,
                        permits: updatedPermits
                    };
                }
            }
            return item;
        }).filter(item => item !== null); // Remove null items
        
        console.log('Updated review queue:', this.reviewQueue);
        
        // Save updated review queue
        localStorage.setItem('reservoirReviewQueue', JSON.stringify(this.reviewQueue));
        
        // Update display
        this.updateReviewQueueDisplay();
    }
    
    async getReservoirSuggestion(wrongReservoirName, permits) {
        try {
            // Use the first permit for the suggestion request
            const firstPermit = permits[0];
            const permitId = firstPermit.id || this.getPermitIdByStatusNo(firstPermit.status_no);
            
            if (!permitId) {
                alert('Unable to find permit ID for suggestion');
                return;
            }
            
            // Show loading message
            const loadingMsg = document.createElement('div');
            loadingMsg.style.cssText = 'position: fixed; top: 20px; right: 20px; background: #3b82f6; color: white; padding: 12px 20px; border-radius: 8px; z-index: 1000; font-weight: 500;';
            loadingMsg.textContent = '🤖 Getting AI suggestion...';
            document.body.appendChild(loadingMsg);
            
            const response = await fetch(`/api/v1/field-corrections/suggest/${permitId}`);
            
            document.body.removeChild(loadingMsg);
            
            if (response.ok) {
                const result = await response.json();
                
                if (result.has_suggestion) {
                    const useIt = confirm(
                        `🤖 AI RESERVOIR SUGGESTION\n\n` +
                        `Current Reservoir: "${wrongReservoirName}"\n` +
                        `AI Suggests: "${result.suggested_field}"\n` +
                        `Affects ${permits.length} permit${permits.length !== 1 ? 's' : ''}\n\n` +
                        `Apply this suggestion to all permits?`
                    );
                    
                    if (useIt) {
                        // Apply the suggestion using the existing correction function
                        await this.applyReservoirCorrection(wrongReservoirName, permits, result.suggested_field);
                    }
                } else {
                    // Show info message
                    const infoMsg = document.createElement('div');
                    infoMsg.style.cssText = 'position: fixed; top: 20px; right: 20px; background: #f59e0b; color: white; padding: 12px 20px; border-radius: 8px; z-index: 1000; font-weight: 500;';
                    infoMsg.textContent = 'ℹ️ No AI suggestion available for this reservoir yet';
                    document.body.appendChild(infoMsg);
                    
                    setTimeout(() => {
                        document.body.removeChild(infoMsg);
                    }, 3000);
                }
                
            } else {
                // Show error message
                const errorMsg = document.createElement('div');
                errorMsg.style.cssText = 'position: fixed; top: 20px; right: 20px; background: #ef4444; color: white; padding: 12px 20px; border-radius: 8px; z-index: 1000; font-weight: 500;';
                errorMsg.textContent = '❌ Failed to get AI suggestion';
                document.body.appendChild(errorMsg);
                
                setTimeout(() => {
                    document.body.removeChild(errorMsg);
                }, 3000);
            }
            
        } catch (error) {
            console.error('AI suggestion error:', error);
            // Show error message
            const errorMsg = document.createElement('div');
            errorMsg.style.cssText = 'position: fixed; top: 20px; right: 20px; background: #ef4444; color: white; padding: 12px 20px; border-radius: 8px; z-index: 1000; font-weight: 500;';
            errorMsg.textContent = `❌ Error getting suggestion: ${error.message}`;
            document.body.appendChild(errorMsg);
            
            setTimeout(() => {
                document.body.removeChild(errorMsg);
            }, 3000);
        }
    }
    
    async applyLearnedCorrections() {
        try {
            const confirmed = confirm(
                '🤖 APPLY LEARNED CORRECTIONS\n\n' +
                'This will automatically apply previously learned field name corrections to similar permits.\n\n' +
                'Continue?'
            );
            
            if (!confirmed) return;
            
            this.showInfo('🤖 Applying learned corrections...');
            
            const response = await fetch('/api/v1/field-corrections/apply-learned', {
                method: 'POST'
            });
            
            if (response.ok) {
                const result = await response.json();
                
                if (result.corrected > 0) {
                    this.showSuccess(`✅ Applied corrections to ${result.corrected} permits!`);
                    setTimeout(() => this.loadPermitData(), 2000);
                } else {
                    this.showInfo('No permits needed corrections');
                }
                
            } else {
                this.showError('Failed to apply learned corrections');
            }
            
        } catch (error) {
            console.error('Apply corrections error:', error);
            this.showError(`Error applying corrections: ${error.message}`);
        }
    }
    
    removeFromReviewQueue(fieldName) {
        this.reviewQueue = this.reviewQueue.filter(item => item.fieldName !== fieldName);
        localStorage.setItem('reservoirReviewQueue', JSON.stringify(this.reviewQueue));
        this.updateReviewQueueDisplay();
    }
    
    updateReviewQueueDisplay() {
        // Only update if we're in the reservoir management modal
        const reviewQueueEl = document.getElementById('reviewQueue');
        if (!reviewQueueEl) return;
        
        // Additional safety check: only update if element is inside a modal
        const modal = reviewQueueEl.closest('.reservoir-manager-modal');
        if (!modal) {
            console.warn('Review queue element found outside of modal context - skipping update');
            return;
        }
        
        if (this.reviewQueue.length === 0) {
            reviewQueueEl.innerHTML = `
                <div style="color: var(--text-secondary); text-align: center; padding: 1rem; font-size: 0.875rem;">
                    No items in review queue
                </div>
            `;
            return;
        }
        
        reviewQueueEl.innerHTML = this.reviewQueue.map(item => `
            <div style="padding: 0.75rem; border: 1px solid var(--border-color); border-radius: 0.5rem; margin-bottom: 0.5rem; background: var(--background-color);">
                <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 0.5rem;">
                    <div style="flex: 1; min-width: 0;">
                        <div style="font-weight: 600; font-size: 0.875rem; color: var(--primary-color); margin-bottom: 0.25rem;">
                            ${item.fieldName}
                        </div>
                        <div style="font-size: 0.75rem; color: var(--text-secondary); margin-bottom: 0.5rem;">
                            Suggested: ${item.suggestedReservoir}
                        </div>
                        <div style="font-size: 0.75rem; color: var(--text-secondary);">
                            ${item.permits.length} permit${item.permits.length !== 1 ? 's' : ''}
                        </div>
                    </div>
                </div>
                <div style="display: flex; gap: 0.5rem; margin-top: 0.5rem;">
                    <button onclick="window.dashboard.reviewReservoirMapping('${item.fieldName.replace(/'/g, "\\'")}', '${item.suggestedReservoir.replace(/'/g, "\\'")}')" 
                            style="flex: 1; padding: 0.5rem; background: var(--gradient-primary); color: white; border: none; border-radius: 0.375rem; cursor: pointer; font-size: 0.75rem;">
                        📋 Review Now
                    </button>
                    <button onclick="window.dashboard.removeFromReviewQueue('${item.fieldName.replace(/'/g, "\\'")}')" 
                            style="padding: 0.5rem; background: var(--error-color); color: white; border: none; border-radius: 0.375rem; cursor: pointer; font-size: 0.75rem;">
                        ✕
                    </button>
                </div>
            </div>
        `).join('');
    }
    
    reviewReservoirMapping(fieldName, suggestedReservoir) {
        // Find permits with this field name for context
        const relatedPermits = this.getPermitsWithFieldName(fieldName);
        
        // Show the mapping modal with additional context
        this.showReservoirReviewModal(fieldName, suggestedReservoir, relatedPermits);
    }
    
    showReservoirReviewModal(fieldName, suggestedReservoir, relatedPermits) {
        const modal = document.createElement('div');
        modal.className = 'fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50';
        modal.style.cssText = 'position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.7); display: flex; align-items: center; justify-content: center; z-index: 1000;';
        
        modal.innerHTML = `
            <div style="background: white; padding: 2rem; border-radius: 0.75rem; max-width: 700px; margin: 1rem; max-height: 80vh; overflow-y: auto;">
                <h3 style="font-size: 1.25rem; font-weight: 600; margin-bottom: 1rem; color: var(--primary-color);">
                    📋 Review Reservoir Mapping
                </h3>
                
                <div style="margin-bottom: 1.5rem;">
                    <p style="margin-bottom: 0.5rem; color: var(--text-secondary);">
                        Field name to map: <strong>"${fieldName}"</strong>
                    </p>
                    <p style="margin-bottom: 1rem; color: var(--text-secondary);">
                        Found in ${relatedPermits.length} permit${relatedPermits.length !== 1 ? 's' : ''}
                    </p>
                </div>
                
                <div style="margin-bottom: 1.5rem; padding: 1rem; background: var(--background-color); border-radius: 0.5rem;">
                    <h4 style="font-size: 1rem; font-weight: 600; margin-bottom: 0.75rem; color: var(--primary-color);">
                        Related Permits:
                    </h4>
                    ${relatedPermits.map(permit => `
                        <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.5rem 0; border-bottom: 1px solid var(--border-color);">
                            <div>
                                <div style="font-weight: 500; font-size: 0.875rem;">${permit.lease_name}</div>
                                <div style="font-size: 0.75rem; color: var(--text-secondary);">${permit.county} County • ${permit.status_no}</div>
                            </div>
                            <a href="${permit.detail_url}" target="_blank" 
                               style="padding: 0.25rem 0.5rem; background: var(--gradient-accent); color: white; text-decoration: none; border-radius: 0.25rem; font-size: 0.75rem;">
                                View Permit
                            </a>
                        </div>
                    `).join('')}
                </div>
                
                <div style="margin-bottom: 1.5rem;">
                    <label style="display: block; font-weight: 500; margin-bottom: 0.5rem;">
                        Reservoir name to display on cards:
                    </label>
                    <input type="text" id="reservoirReviewInput" value="${suggestedReservoir}" 
                           style="width: 100%; padding: 0.75rem; border: 1px solid var(--border-color); border-radius: 0.5rem;">
                </div>
                
                <div style="display: flex; gap: 0.75rem; justify-content: flex-end;">
                    <button id="cancelReview" style="padding: 0.75rem 1rem; background: var(--surface-color); border: 1px solid var(--border-color); border-radius: 0.5rem; cursor: pointer;">
                        Cancel
                    </button>
                    <button id="keepInQueue" style="padding: 0.75rem 1rem; background: var(--gradient-accent); color: white; border: none; border-radius: 0.5rem; cursor: pointer;">
                        Keep in Queue
                    </button>
                    <button id="saveReviewMapping" style="padding: 0.75rem 1rem; background: var(--gradient-primary); color: white; border: none; border-radius: 0.5rem; cursor: pointer;">
                        Save Mapping
                    </button>
                </div>
            </div>
        `;
        
        document.body.appendChild(modal);
        
        // Event handlers
        modal.querySelector('#cancelReview').addEventListener('click', () => {
            modal.remove();
        });
        
        modal.querySelector('#keepInQueue').addEventListener('click', () => {
            modal.remove();
        });
        
        modal.querySelector('#saveReviewMapping').addEventListener('click', () => {
            const reservoirName = modal.querySelector('#reservoirReviewInput').value.trim();
            if (reservoirName) {
                this.reservoirMapping[fieldName] = reservoirName;
                localStorage.setItem('reservoirMapping', JSON.stringify(this.reservoirMapping));
                this.removeFromReviewQueue(fieldName);
                this.renderPermitCards(); // Re-render with new mapping
                
                // Refresh the current tab display to show updated content
                const activeTab = document.querySelector('.reservoir-tab.active');
                if (activeTab) {
                    const tabName = activeTab.id.replace('Tab', '');
                    this.switchReservoirTab(tabName);
                }
                
                this.showSuccess(`Reservoir mapping saved: "${fieldName}" → "${reservoirName}"`);
            }
            modal.remove();
        });
        
        // Focus the input
        setTimeout(() => {
            modal.querySelector('#reservoirReviewInput').focus();
        }, 100);
    }
    
    openReservoirManager() {
        try {
            console.log('Opening Reservoir Manager...');
            
            // MEMORY LEAK FIX: Check for existing modal and remove it first
            const existingModal = document.querySelector('.reservoir-manager-modal');
            if (existingModal) {
                console.log('Removing existing reservoir manager modal');
                existingModal.remove();
            }
            
        // Create the comprehensive reservoir management modal
        const modal = document.createElement('div');
        modal.className = 'reservoir-manager-modal fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50';
        modal.style.cssText = 'position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.7); display: flex; align-items: center; justify-content: center; z-index: 1000;';
        
        modal.innerHTML = `
            <div style="background: white; border-radius: 1rem; width: 95vw; height: 90vh; max-width: 1400px; position: relative; display: flex; flex-direction: column; box-shadow: 0 25px 50px -12px rgb(0 0 0 / 0.25);">
                <div style="padding: 1.5rem; border-bottom: 1px solid var(--border-color); display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <h2 style="margin: 0; font-size: 1.5rem; font-weight: 600; color: var(--primary-color);">
                            🎯 Reservoir Management Center
                        </h2>
                        <p style="margin: 0.5rem 0 0 0; color: var(--text-secondary); font-size: 0.875rem;">
                            Manage all field name mappings and reservoir classifications
                        </p>
                    </div>
                    <button onclick="this.closest('.fixed').remove()" 
                            style="padding: 0.5rem; background: none; border: none; font-size: 1.5rem; cursor: pointer; color: var(--text-secondary);">
                        ✕
                    </button>
                </div>
                
                <div style="display: flex; border-bottom: 1px solid var(--border-color);">
                    <button id="savedTab" class="reservoir-tab active" onclick="window.dashboard.switchReservoirTab('saved')">
                        ✅ Saved Mappings (${Object.keys(this.reservoirMapping).length})
                    </button>
                    <button id="reviewTab" class="reservoir-tab" onclick="window.dashboard.switchReservoirTab('review')">
                        📋 Under Review (${this.reviewQueue.length})
                    </button>
                    <button id="flaggedTab" class="reservoir-tab" onclick="window.dashboard.switchReservoirTab('flagged')">
                        🚩 Flagged for Re-processing (<span id="flaggedCount">0</span>)
                    </button>
                    <button id="cancelledTab" class="reservoir-tab" onclick="window.dashboard.switchReservoirTab('cancelled')">
                        ❌ Cancelled (${this.cancelledMappings.length})
                    </button>
                </div>
                
                <div style="flex: 1; overflow: hidden; display: flex;">
                    <div style="flex: 1; overflow-y: auto; padding: 1rem;">
                        <div id="reservoirTabContent">
                            Loading...
                        </div>
                    </div>
                </div>
                
                <div style="padding: 1rem; border-top: 1px solid var(--border-color); display: flex; justify-content: space-between; align-items: center;">
                    <button onclick="window.dashboard.showAddManualMappingModal()" 
                            style="padding: 0.75rem 1rem; background: #10b981; color: white; border: none; border-radius: 0.375rem; cursor: pointer;">
                        ➕ Add Manual Mapping
                    </button>
                    <div>
                    <button onclick="window.dashboard.exportReservoirMappings()" 
                            style="padding: 0.75rem 1rem; background: var(--gradient-accent); color: white; border: none; border-radius: 0.375rem; cursor: pointer; margin-right: 0.75rem;">
                        📥 Export Mappings
                    </button>
                    <button onclick="this.closest('.fixed').remove()" 
                            style="padding: 0.75rem 1rem; background: var(--gradient-primary); color: white; border: none; border-radius: 0.375rem; cursor: pointer;">
                        Close
                    </button>
                    </div>
                </div>
            </div>
        `;
        
        document.body.appendChild(modal);
        
        // Load the saved mappings tab by default
        this.switchReservoirTab('saved');
        
        // MEMORY LEAK FIX: Named function for proper cleanup
        const closeOnOutsideClick = (e) => {
            if (e.target === modal) {
                modal.removeEventListener('click', closeOnOutsideClick);
                modal.remove();
            }
        };
        modal.addEventListener('click', closeOnOutsideClick);
        
        console.log('Reservoir Manager opened successfully');
        
        } catch (error) {
            console.error('Error opening Reservoir Manager:', error);
            this.showSafeMessage('Error opening Reservoir Manager: ' + error.message, 'error');
        }
    }
    
    showAddManualMappingModal() {
        const modal = document.createElement('div');
        modal.style.cssText = 'position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.7); display: flex; align-items: center; justify-content: center; z-index: 1001;';
        
        modal.innerHTML = `
            <div style="background: white; border-radius: 1rem; width: 90vw; max-width: 600px; padding: 2rem; box-shadow: 0 25px 50px -12px rgb(0 0 0 / 0.25);">
                <div style="margin-bottom: 1.5rem;">
                    <h2 style="margin: 0 0 0.5rem 0; font-size: 1.5rem; font-weight: 600; color: var(--primary-color);">
                        ➕ Add Manual Reservoir Mapping
                    </h2>
                    <p style="margin: 0; color: var(--text-secondary); font-size: 0.875rem;">
                        Paste the raw field name from RRC records and define what reservoir it should map to.
                    </p>
                </div>
                
                <div style="margin-bottom: 1.5rem;">
                    <label style="display: block; font-weight: 500; margin-bottom: 0.5rem; color: var(--text-primary);">
                        Raw Field Name (as shown in RRC records):
                    </label>
                    <textarea id="rawFieldName" placeholder="Paste the exact field name from RRC records here..." 
                              style="width: 100%; height: 80px; padding: 0.75rem; border: 1px solid var(--border-color); border-radius: 0.5rem; font-family: monospace; font-size: 0.875rem; resize: vertical;"></textarea>
                    <div style="font-size: 0.75rem; color: var(--text-secondary); margin-top: 0.25rem;">
                        Example: "HAWKVILLE (AUSTIN CHALK)" or "Please pay for the SWR 37 exception fee"
                    </div>
                </div>
                
                <div style="margin-bottom: 1.5rem;">
                    <label style="display: block; font-weight: 500; margin-bottom: 0.5rem; color: var(--text-primary);">
                        Correct Reservoir Name:
                    </label>
                    <input type="text" id="correctReservoir" placeholder="Enter the correct geological reservoir name..." 
                           style="width: 100%; padding: 0.75rem; border: 1px solid var(--border-color); border-radius: 0.5rem; font-size: 0.875rem;">
                    <div style="font-size: 0.75rem; color: var(--text-secondary); margin-top: 0.25rem;">
                        Example: "AUSTIN CHALK", "EAGLE FORD", "WOLFCAMP", etc.
                    </div>
                </div>
                
                <div style="display: flex; gap: 1rem; justify-content: flex-end;">
                    <button onclick="this.closest('.fixed').remove()" 
                            style="padding: 0.75rem 1.5rem; background: #6b7280; color: white; border: none; border-radius: 0.5rem; cursor: pointer;">
                        Cancel
                    </button>
                    <button onclick="window.dashboard.saveManualMapping()" 
                            style="padding: 0.75rem 1.5rem; background: #10b981; color: white; border: none; border-radius: 0.5rem; cursor: pointer; font-weight: 600;">
                        💾 Save Mapping
                    </button>
                </div>
            </div>
        `;
        
        document.body.appendChild(modal);
        
        // Focus the first input
        setTimeout(() => {
            modal.querySelector('#rawFieldName').focus();
        }, 100);
        
        // Close on outside click
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                modal.remove();
            }
        });
    }
    
    saveManualMapping() {
        const rawFieldName = document.getElementById('rawFieldName').value.trim();
        const correctReservoir = document.getElementById('correctReservoir').value.trim();
        
        if (!rawFieldName || !correctReservoir) {
            alert('Please fill in both the raw field name and correct reservoir name.');
            return;
        }
        
        // Add to saved mappings
        this.addToSavedMappings(rawFieldName, correctReservoir);
        
        // Show success message
        const successMsg = document.createElement('div');
        successMsg.style.cssText = 'position: fixed; top: 20px; right: 20px; background: #10b981; color: white; padding: 12px 20px; border-radius: 8px; z-index: 1002; font-weight: 500;';
        successMsg.textContent = `✅ Manual mapping saved: "${rawFieldName}" → "${correctReservoir}"`;
        document.body.appendChild(successMsg);
        
        // Close modal
        const modal = document.querySelector('.fixed');
        if (modal) modal.remove();
        
        // Refresh the saved mappings tab if Reservoir Manager is still open
        setTimeout(() => {
            if (successMsg && successMsg.parentNode) {
                document.body.removeChild(successMsg);
            }
            // Only switch tabs if the Reservoir Manager modal is still open
            const reservoirModal = document.querySelector('.reservoir-manager-modal');
            if (reservoirModal) {
                this.switchReservoirTab('saved');
            }
        }, 2000);
    }
    
    openManualMappingForPermit(permit) {
        const modal = document.createElement('div');
        modal.className = 'manual-mapping-modal fixed';
        modal.style.cssText = 'position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.7); display: flex; align-items: center; justify-content: center; z-index: 1001;';
        
        // Pre-populate with permit data - try multiple field name sources
        const currentFieldName = permit.currentFieldName || permit.field_name || permit.fieldName || 'UNKNOWN_FIELD';
        const permitUrl = permit.detail_url || '';
        const statusNo = permit.status_no || '';
        
        modal.innerHTML = `
            <div style="background: white; border-radius: 1rem; width: 90vw; max-width: 700px; padding: 2rem; box-shadow: 0 25px 50px -12px rgb(0 0 0 / 0.25);">
                <div style="margin-bottom: 1.5rem;">
                    <h2 style="margin: 0 0 0.5rem 0; font-size: 1.5rem; font-weight: 600; color: var(--primary-color);">
                        ➕ Manual Mapping for ${permit.lease_name || 'Permit'}
                    </h2>
                    <p style="margin: 0; color: var(--text-secondary); font-size: 0.875rem;">
                        Status: ${statusNo} • Copy the correct field name from RRC records and define the reservoir.
                    </p>
                    ${permitUrl ? `
                        <div style="margin-top: 0.75rem;">
                            <a href="${permitUrl}" target="_blank" 
                               style="display: inline-block; padding: 0.5rem 1rem; background: var(--primary-color); color: white; text-decoration: none; border-radius: 0.375rem; font-size: 0.875rem;">
                                📄 Open RRC Permit Details
                            </a>
                        </div>
                    ` : `
                        <div style="margin-top: 0.75rem; padding: 0.5rem; background: #f3f4f6; border-radius: 0.375rem; font-size: 0.75rem; color: var(--text-secondary);">
                            <strong>Note:</strong> This is a historical permit imported for trend analysis. No RRC link available.
                        </div>
                    `}
                </div>
                
                <div style="margin-bottom: 1.5rem;">
                    <label style="display: block; font-weight: 500; margin-bottom: 0.5rem; color: var(--text-primary);">
                        Current Field Name (from permit):
                    </label>
                    <div style="padding: 0.75rem; background: #f3f4f6; border: 1px solid var(--border-color); border-radius: 0.5rem; font-family: monospace; font-size: 0.875rem; color: #6b7280;">
                        ${currentFieldName}
                    </div>
                </div>
                
                <div style="margin-bottom: 1.5rem;">
                    <label style="display: block; font-weight: 500; margin-bottom: 0.5rem; color: var(--text-primary);">
                        Correct Field Name (copy from RRC records):
                    </label>
                    <textarea id="correctFieldName" placeholder="Copy and paste the exact field name from RRC permit details..." 
                              style="width: 100%; height: 80px; padding: 0.75rem; border: 1px solid var(--border-color); border-radius: 0.5rem; font-family: monospace; font-size: 0.875rem; resize: vertical;"></textarea>
                    <div style="font-size: 0.75rem; color: var(--text-secondary); margin-top: 0.25rem;">
                        Example: "PAN PETRO (CLEVELAND)" or "SPRABERRY (TREND AREA)"
                    </div>
                </div>
                
                <div style="margin-bottom: 1.5rem;">
                    <label style="display: block; font-weight: 500; margin-bottom: 0.5rem; color: var(--text-primary);">
                        Correct Reservoir Name:
                    </label>
                    <input type="text" id="correctReservoirName" placeholder="Enter the geological reservoir name..." 
                           style="width: 100%; padding: 0.75rem; border: 1px solid var(--border-color); border-radius: 0.5rem; font-size: 0.875rem;">
                    <div style="font-size: 0.75rem; color: var(--text-secondary); margin-top: 0.25rem;">
                        Example: "CLEVELAND", "SPRABERRY", "EAGLE FORD", etc.
                    </div>
                </div>
                
                <div style="display: flex; gap: 1rem; justify-content: flex-end;">
                    <button onclick="this.closest('.fixed').remove()" 
                            style="padding: 0.75rem 1.5rem; background: #6b7280; color: white; border: none; border-radius: 0.5rem; cursor: pointer;">
                        Cancel
                    </button>
                    <button onclick="window.dashboard.savePermitManualMapping('${permit.status_no}', '${currentFieldName.replace(/'/g, "\\'")}', '${permitUrl.replace(/'/g, "\\'")}')" 
                            style="padding: 0.75rem 1.5rem; background: #10b981; color: white; border: none; border-radius: 0.5rem; cursor: pointer; font-weight: 600;">
                        💾 Save & Update Database
                    </button>
                </div>
            </div>
        `;
        
        document.body.appendChild(modal);
        
        // Add keyboard support (Escape to close)
        const handleKeydown = (e) => {
            if (e.key === 'Escape') {
                modal.remove();
                document.removeEventListener('keydown', handleKeydown);
            }
        };
        document.addEventListener('keydown', handleKeydown);
        
        // Remove event listener when modal is removed
        const originalRemove = modal.remove;
        modal.remove = function() {
            document.removeEventListener('keydown', handleKeydown);
            originalRemove.call(this);
        };
        
        // Focus the correct field name input
        setTimeout(() => {
            modal.querySelector('#correctFieldName').focus();
        }, 100);
        
        // Add keyboard support - Enter to save, Escape to close
        modal.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                // Trigger the save function
                this.savePermitManualMapping(statusNo, currentFieldName, permitUrl);
            } else if (e.key === 'Escape') {
                e.preventDefault();
                modal.remove();
            }
        });
        
        // Close on outside click
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                modal.remove();
            }
        });
    }
    
    async savePermitManualMapping(statusNo, oldFieldName, permitUrl) {
        const correctFieldName = document.getElementById('correctFieldName').value.trim();
        const correctReservoir = document.getElementById('correctReservoirName').value.trim();
        
        if (!correctFieldName || !correctReservoir) {
            alert('Please fill in both the correct field name and reservoir name.');
            return;
        }
        
        if (!oldFieldName) {
            console.error('oldFieldName is missing or empty!');
            alert('Error: Missing original field name. Please try refreshing the page and try again.');
            return;
        }
        
        try {
            // 1. Save to local mappings
            this.addToSavedMappings(correctFieldName, correctReservoir);
            
            // 2. Update the specific permit in database
            const response = await fetch('/api/v1/field-corrections/correct', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    status_no: statusNo,
                    wrong_field: oldFieldName,
                    correct_field: correctFieldName,
                    correct_reservoir: correctReservoir,
                    detail_url: permitUrl,
                    html_context: ""
                })
            });
            
            if (!response.ok) {
                // Try to get detailed error message from server
                let errorMessage = `HTTP ${response.status}: ${response.statusText}`;
                try {
                    const errorData = await response.json();
                    if (errorData.detail) {
                        errorMessage = `${errorMessage} - ${errorData.detail}`;
                    }
                } catch (e) {
                    // If we can't parse the error response, use the basic message
                }
                throw new Error(errorMessage);
            }
            
            const result = await response.json();
            
            // 3. Check for other permits with same wrong field name and show confirmation
            const otherPermitsCount = await this.countPermitsWithSameFieldName(oldFieldName, statusNo);
            
            if (otherPermitsCount > 0) {
                const confirmed = await this.showBulkUpdateConfirmation(otherPermitsCount, oldFieldName, correctFieldName);
                if (confirmed) {
                    await this.crossReferenceAndUpdatePermits(oldFieldName, correctFieldName);
                }
            }
            
            // Show success message
            const successMsg = document.createElement('div');
            successMsg.style.cssText = 'position: fixed; top: 20px; right: 20px; background: #10b981; color: white; padding: 12px 20px; border-radius: 8px; z-index: 1002; font-weight: 500;';
            successMsg.textContent = `✅ Mapping saved and database updated: "${correctFieldName}" → "${correctReservoir}"`;
            document.body.appendChild(successMsg);
            
            // Auto-remove success message after 4 seconds
            setTimeout(() => {
                if (successMsg.parentNode) {
                    successMsg.remove();
                }
            }, 4000);
            
            // Close modal - use specific class name for reliability
            const modal = document.querySelector('.manual-mapping-modal');
            if (modal) {
                modal.remove();
                console.log('Manual mapping modal closed successfully');
            } else {
                console.warn('Manual mapping modal not found for closing');
            }
            
            // Update local permit data immediately to prevent orange card issue
            const permit = this.permits.find(p => p.status_no === statusNo);
            if (permit) {
                permit.field_name = correctFieldName;
                console.log(`Updated local permit ${statusNo} field_name to: ${correctFieldName}`);
            }
            
            // Update reservoir mapping to prevent "new reservoir" detection
            this.reservoirMapping[correctFieldName] = correctReservoir;
            localStorage.setItem('reservoirMapping', JSON.stringify(this.reservoirMapping));
            
            // Remove from review queue
            this.removeSinglePermitFromReview(oldFieldName, statusNo);
            
            // Refresh UI immediately
            this.applyFilters();
            
            // Refresh data from server (but don't wait for it)
            setTimeout(() => {
                if (successMsg && successMsg.parentNode) {
                    document.body.removeChild(successMsg);
                }
                this.loadPermits(); // Refresh permits to show updated data
                
                // Refresh reservoir manager if open
                const reservoirModal = document.querySelector('.reservoir-manager-modal');
                if (reservoirModal) {
                    this.switchReservoirTab('saved');
                }
            }, 3000);
            
        } catch (error) {
            console.error('Error saving manual mapping:', error);
            alert(`Error saving mapping: ${error.message}`);
        }
    }
    
    async crossReferenceAndUpdatePermits(wrongFieldName, correctFieldName) {
        try {
            // Find all permits with the same wrong field name and update them
            const response = await fetch('/api/v1/permits/bulk-update-field', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    wrong_field: wrongFieldName,
                    correct_field: correctFieldName
                })
            });
            
            if (response.ok) {
                const result = await response.json();
                console.log(`Cross-reference update: ${result.updated_count} permits updated`);
            } else {
                console.warn('Cross-reference update failed, but manual mapping was saved');
            }
        } catch (error) {
            console.warn('Cross-reference update error:', error);
            // Don't fail the whole operation if cross-reference fails
        }
    }
    
    isValidReservoirName(fieldName) {
        if (!fieldName || fieldName.length < 3) return false;
        
        const name = fieldName.toLowerCase();
        
        // Common geological formations and reservoir names
        const validReservoirTerms = [
            'austin chalk', 'eagle ford', 'wolfcamp', 'spraberry', 'barnett',
            'bone spring', 'delaware', 'permian', 'woodford', 'haynesville',
            'marcellus', 'utica', 'bakken', 'niobrara', 'cleveland', 'granite wash',
            'atoka', 'canyon', 'strawn', 'bend', 'frio', 'jackson', 'yegua', 'wilcox',
            'cotton valley', 'travis peak', 'hosston', 'sligo', 'james lime',
            'cherry canyon', 'clear fork', 'palo pinto', 'devonian', 'mississippian',
            'pennsylvanian', 'ordovician', 'cambrian', 'ellenburger', 'simpson',
            'viola', 'hunton', 'woodford', 'chester', 'morrow', 'cisco', 'canyon',
            'strawn', 'atoka', 'desmoinesian', 'missourian', 'virgilian', 'wolfcampian'
        ];
        
        // Check if field name contains valid geological terms
        const containsValidTerm = validReservoirTerms.some(term => name.includes(term));
        
        // Check for formation pattern (something in parentheses)
        const hasFormationPattern = fieldName.includes('(') && fieldName.includes(')');
        
        // Exclude obvious non-reservoir patterns
        const invalidPatterns = [
            'additional problems', 'exactly as shown', 'commission staff',
            'expresses no opinion', 'please pay', 'exception fee', 'revised plat',
            'changed survey', 'allocation wells', 'drilled concurrent',
            'recompletion', 'completion', 'interval', 'tracts shown', 'tracts listed'
        ];
        
        const hasInvalidPattern = invalidPatterns.some(pattern => name.includes(pattern));
        
        return (containsValidTerm || hasFormationPattern) && !hasInvalidPattern;
    }
    
    isInjectionWell(permit) {
        if (!permit) return false;
        
        const filingPurpose = (permit.filing_purpose || '').toLowerCase();
        const currentQueue = (permit.current_queue || '').toLowerCase();
        const leaseName = (permit.lease_name || '').toLowerCase();
        
        // Check for injection well indicators
        const injectionIndicators = [
            'injection well',
            'injection',
            'disposal well',
            'disposal',
            'saltwater disposal',
            'swd',
            'water injection',
            'enhanced recovery',
            'secondary recovery',
            'waterflooding',
            'water flood',
            'co2 injection',
            'gas injection'
        ];
        
        return injectionIndicators.some(indicator => 
            filingPurpose.includes(indicator) || 
            currentQueue.includes(indicator) ||
            leaseName.includes(indicator)
        );
    }
    
    async removeInjectionWell(permit) {
        console.log('removeInjectionWell called with permit:', permit);
        
        if (!confirm(`Are you sure you want to permanently delete this injection well from the database?\n\nPermit: ${permit.status_no}\nOperator: ${permit.operator_name}\nLease: ${permit.lease_name}\n\nThis action cannot be undone.`)) {
            console.log('User cancelled deletion');
            return;
        }
        
        console.log(`Starting deletion process for permit ${permit.status_no}`);
        
        try {
            console.log(`Sending DELETE request to /api/v1/permits/${permit.status_no}`);
            
            const response = await fetch(`/api/v1/permits/${permit.status_no}`, {
                method: 'DELETE',
                headers: {
                    'Content-Type': 'application/json',
                }
            });
            
            console.log(`DELETE response status: ${response.status} ${response.statusText}`);
            
            if (!response.ok) {
                let errorMessage = `HTTP ${response.status}: ${response.statusText}`;
                try {
                    const errorData = await response.json();
                    if (errorData.detail) {
                        errorMessage = `${errorMessage} - ${errorData.detail}`;
                    }
                } catch (e) {
                    // If we can't parse the error response, use the basic message
                }
                throw new Error(errorMessage);
            }
            
            const responseData = await response.json();
            console.log('DELETE response data:', responseData);
            
            // Show success message
            const successMsg = document.createElement('div');
            successMsg.style.cssText = 'position: fixed; top: 20px; right: 20px; background: #dc2626; color: white; padding: 12px 20px; border-radius: 8px; z-index: 1002; font-weight: 500;';
            successMsg.textContent = `🗑️ Injection well deleted: ${permit.status_no}`;
            document.body.appendChild(successMsg);
            
            // Auto-remove success message after 4 seconds
            setTimeout(() => {
                if (successMsg.parentNode) {
                    successMsg.remove();
                }
            }, 4000);
            
            // Remove the deleted permit from local data immediately
            console.log(`Current permits array length: ${this.permits.length}`);
            console.log(`Looking for permit ${permit.status_no} in local data...`);
            
            const permitIndex = this.permits.findIndex(p => p.status_no === permit.status_no);
            if (permitIndex !== -1) {
                console.log(`Found permit ${permit.status_no} at index ${permitIndex}, removing from local data`);
                console.log(`Permit data:`, this.permits[permitIndex]);
                this.permits.splice(permitIndex, 1);
                console.log(`New permits array length: ${this.permits.length}`);
                console.log('Calling applyFilters() to re-render...');
                this.applyFilters(); // Re-render without the deleted permit
            } else {
                console.warn(`Permit ${permit.status_no} not found in local data for removal`);
                console.log('All permit status_no values in local data:', this.permits.map(p => p.status_no));
            }
            
            // Also refresh from server to ensure consistency
            setTimeout(() => {
                console.log('Refreshing permits from server after deletion...');
                this.loadPermits();
            }, 1000);
            
        } catch (error) {
            console.error('Error removing injection well:', error);
            alert(`Failed to remove injection well: ${error.message}`);
        }
    }

    // Check if field name is incorrectly parsed from comments/remarks
    isIncorrectlyParsedFieldName(fieldName) {
        if (!fieldName) return false;
        
        const incorrectPatterns = [
            // Dates and times
            /\d{2}\/\d{2}\/\d{4}/,
            /\d{1,2}:\d{2}:\d{2}/,
            /(AM|PM)/i,
            
            // Commission staff comments
            /commission staff/i,
            /expresses no opinion/i,
            /staff expresses/i,
            /no opinion/i,
            
            // Application-related text
            /application to/i,
            /application is/i,
            /amend surface/i,
            /surface location/i,
            
            // Problem/issue indicators
            /additional problems/i,
            /there are additional/i,
            /problems with/i,
            
            // Generic administrative text
            /please pay/i,
            /exception fee/i,
            /re-entry permit/i,
            /revised plat/i,
            /changed.*survey/i,
            /allocation wells/i,
            /drilled concurre/i,
            
            // Long sentences (likely comments)
            /^.{100,}/, // More than 100 characters is likely a comment
            
            // Contains multiple sentences
            /\.\s+[A-Z]/, // Period followed by space and capital letter
            
            // Parenthetical timestamps
            /\(\s*\d{2}\/\d{2}\/\d{4}\s+\d{1,2}:\d{2}:\d{2}\s*(AM|PM)?\s*\)/i
        ];
        
        return incorrectPatterns.some(pattern => pattern.test(fieldName));
    }

    // Check if permit needs reservoir management (show Manage Reservoir button)
    needsReservoirManagement(permit) {
        if (!permit.field_name) return false;
        
        const fieldName = permit.field_name;
        const extractedReservoir = this.extractReservoir(fieldName);
        
        // Show button if:
        // 1. Reservoir shows as "UNKNOWN"
        if (extractedReservoir === 'UNKNOWN') return true;
        
        // 2. Field name is incorrectly parsed (timestamps, comments, etc.)
        if (this.isIncorrectlyParsedFieldName(fieldName)) return true;
        
        // 3. Field name contains timestamp patterns
        if (/\d{2}\/\d{2}\/\d{4}\s+\d{1,2}:\d{2}:\d{2}\s*(AM|PM)?/i.test(fieldName)) return true;
        
        // 4. Field name is clearly not a geological formation
        const nonGeologicalPatterns = [
            /^unknown$/i,
            /^-$/,
            /^n\/a$/i,
            /^not available$/i,
            /^pending$/i,
            /^tbd$/i,
            /^to be determined$/i
        ];
        
        if (nonGeologicalPatterns.some(pattern => pattern.test(fieldName))) return true;
        
        // 5. Field name is not already mapped and doesn't look like a valid reservoir
        if (!this.reservoirMapping[fieldName] && !this.isValidReservoirName(fieldName)) {
            return true;
        }
        
        return false;
    }
    
    async acceptNewReservoir(permit) {
        try {
            const fieldName = permit.field_name || '';
            const statusNo = permit.status_no || '';
            const permitUrl = permit.detail_url || '';
            
            // Extract reservoir name from field name
            let reservoirName = fieldName;
            
            // If it has parentheses, extract the part in parentheses
            if (fieldName.includes('(') && fieldName.includes(')')) {
                const match = fieldName.match(/\(([^)]+)\)/);
                if (match) {
                    reservoirName = match[1].trim();
                }
            }
            
            // Clean up common suffixes
            reservoirName = reservoirName.replace(/\s+(trend\s+area|formation|shale|chalk|sand|lime)$/i, '').trim();
            
            // Add to reservoir mappings (this is what the card rendering checks)
            this.reservoirMapping[fieldName] = reservoirName.toUpperCase();
            localStorage.setItem('reservoirMapping', JSON.stringify(this.reservoirMapping));
            
            // Save to database for learning and persistence
            try {
                const response = await fetch('/api/v1/field-corrections/correct', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        status_no: statusNo,
                        wrong_field: fieldName,
                        correct_field: fieldName, // Field name is correct, just accepting it
                        correct_reservoir: reservoirName.toUpperCase(),
                        detail_url: permitUrl,
                        html_context: ""
                    })
                });
                
                if (!response.ok) {
                    console.warn(`Failed to save acceptance to database: ${response.status}`);
                }
            } catch (dbError) {
                console.warn('Failed to save acceptance to database:', dbError);
                // Don't fail the whole operation if database save fails
            }
            
            // Show success message
            const successMsg = document.createElement('div');
            successMsg.style.cssText = 'position: fixed; top: 20px; right: 20px; background: #10b981; color: white; padding: 12px 20px; border-radius: 8px; z-index: 1002; font-weight: 500;';
            successMsg.textContent = `✅ Accepted reservoir: "${fieldName}" → "${reservoirName.toUpperCase()}"`;
            document.body.appendChild(successMsg);
            
            // Immediately refresh to update card formatting
            this.applyFilters(); // Re-render cards with updated mappings
            
            // Remove success message after 3 seconds
            setTimeout(() => {
                if (successMsg && successMsg.parentNode) {
                    document.body.removeChild(successMsg);
                }
            }, 3000);
            
        } catch (error) {
            console.error('Error accepting new reservoir:', error);
            alert(`Error accepting reservoir: ${error.message}`);
        }
    }
    
    switchReservoirTab(tabName) {
        // Safety check: ensure we're in the Reservoir Manager modal
        const reservoirModal = document.querySelector('.reservoir-manager-modal');
        if (!reservoirModal) {
            console.log('Reservoir Manager modal not found, skipping tab switch');
            return;
        }
        
        // Update tab buttons
        document.querySelectorAll('.reservoir-tab').forEach(tab => {
            tab.classList.remove('active');
        });
        
        const tabElement = document.getElementById(`${tabName}Tab`);
        if (tabElement) {
            tabElement.classList.add('active');
        }
        
        // Load content for the selected tab
        const contentDiv = document.getElementById('reservoirTabContent');
        if (!contentDiv) {
            console.log('Reservoir tab content div not found');
            return;
        }
        
        switch(tabName) {
            case 'saved':
                this.loadSavedMappingsContent(contentDiv);
                break;
            case 'review':
                this.loadReviewQueueContent(contentDiv);
                break;
            case 'flagged':
                this.loadFlaggedPermitsContent(contentDiv);
                break;
            case 'cancelled':
                this.loadCancelledMappingsContent(contentDiv);
                break;
        }
    }
    
    loadSavedMappingsContent(contentDiv) {
        const savedMappings = Object.entries(this.reservoirMapping);
        
        if (savedMappings.length === 0) {
            contentDiv.innerHTML = `
                <div style="text-align: center; padding: 3rem; color: var(--text-secondary);">
                    <h3>No Saved Mappings</h3>
                    <p>Field name mappings you save will appear here</p>
                </div>
            `;
            return;
        }
        
        // PERFORMANCE FIX: Limit initial display to prevent memory spike
        const INITIAL_DISPLAY_LIMIT = 20;
        const showAll = savedMappings.length <= INITIAL_DISPLAY_LIMIT;
        const displayMappings = showAll ? savedMappings : savedMappings.slice(0, INITIAL_DISPLAY_LIMIT);
        
        // Group mappings by reservoir
        const groupedMappings = {};
        displayMappings.forEach(([fieldName, reservoir]) => {
            if (!groupedMappings[reservoir]) {
                groupedMappings[reservoir] = [];
            }
            groupedMappings[reservoir].push(fieldName);
        });
        
        // Sort reservoirs alphabetically
        const sortedReservoirs = Object.keys(groupedMappings).sort();
        
        contentDiv.innerHTML = `
            <div style="display: grid; gap: 1.5rem;">
                ${sortedReservoirs.map(reservoir => `
                    <div style="border: 1px solid var(--border-color); border-radius: 0.75rem; overflow: hidden; background: var(--surface-color);">
                        <div style="padding: 1rem; background: var(--gradient-primary); color: white;">
                            <h3 style="margin: 0; font-size: 1rem; font-weight: 600; display: flex; align-items: center; gap: 0.5rem;">
                                <span class="reservoir-display" style="background: rgba(255,255,255,0.2); padding: 0.25rem 0.5rem; border-radius: 0.25rem; font-size: 0.75rem;">
                                    ${reservoir}
                                </span>
                                <span style="font-size: 0.875rem; opacity: 0.9;">
                                    (${groupedMappings[reservoir].length} field${groupedMappings[reservoir].length !== 1 ? 's' : ''})
                                </span>
                            </h3>
                        </div>
                        <div style="padding: 1rem;">
                            <div style="display: grid; gap: 0.75rem;">
                                ${groupedMappings[reservoir].map(fieldName => `
                                    <div style="padding: 0.75rem; border: 1px solid var(--border-color); border-radius: 0.5rem; background: var(--background-color); display: flex; justify-content: space-between; align-items: center;">
                                        <div style="flex: 1; min-width: 0;">
                                            <div style="font-weight: 500; font-size: 0.875rem; color: var(--text-primary); margin-bottom: 0.25rem;">
                                                ${fieldName}
                                            </div>
                                            <div style="font-size: 0.75rem; color: var(--text-secondary);">
                                                Maps to: <strong>${reservoir}</strong>
                                            </div>
                                        </div>
                                        <div style="display: flex; gap: 0.5rem; margin-left: 1rem;">
                                            <button onclick="window.dashboard.editReservoirMapping('${fieldName.replace(/'/g, "\\'")}', '${reservoir.replace(/'/g, "\\'")}')" 
                                                    style="padding: 0.375rem 0.75rem; background: var(--gradient-primary); color: white; border: none; border-radius: 0.25rem; cursor: pointer; font-size: 0.75rem;">
                                                ✏️ Edit
                                            </button>
                                            <button onclick="window.dashboard.flagIncorrectFieldName('${fieldName.replace(/'/g, "\\'")}')" 
                                                    style="padding: 0.375rem 0.75rem; background: #ff6b35; color: white; border: none; border-radius: 0.25rem; cursor: pointer; font-size: 0.75rem;" 
                                                    title="Report this field name as incorrectly parsed">
                                                🚩 Report Incorrect
                                            </button>
                                            <button onclick="window.dashboard.moveToReview('${fieldName.replace(/'/g, "\\'")}', '${reservoir.replace(/'/g, "\\'")}')" 
                                                    style="padding: 0.375rem 0.75rem; background: var(--gradient-accent); color: white; border: none; border-radius: 0.25rem; cursor: pointer; font-size: 0.75rem;">
                                                📋 Review
                                            </button>
                                            <button onclick="window.dashboard.deleteReservoirMapping('${fieldName.replace(/'/g, "\\'")}')" 
                                                    style="padding: 0.375rem 0.75rem; background: var(--error-color); color: white; border: none; border-radius: 0.25rem; cursor: pointer; font-size: 0.75rem;">
                                                🗑️
                                            </button>
                                        </div>
                                    </div>
                                `).join('')}
                            </div>
                        </div>
                    </div>
                `).join('')}
            </div>
            ${!showAll ? `
                <div style="text-align: center; padding: 2rem; border-top: 1px solid var(--border-color); margin-top: 1rem;">
                    <p style="color: var(--text-secondary); margin-bottom: 1rem;">
                        Showing ${displayMappings.length} of ${savedMappings.length} mappings
                    </p>
                    <button onclick="window.dashboard.loadAllSavedMappings()" 
                            style="padding: 0.75rem 1.5rem; background: var(--gradient-primary); color: white; border: none; border-radius: 0.375rem; cursor: pointer; font-size: 0.875rem;">
                        📋 Show All ${savedMappings.length} Mappings
                    </button>
                </div>
            ` : ''}
        `;
        
        // Store reference for "Show All" functionality
        this._allSavedMappings = savedMappings;
    }
    
    loadAllSavedMappings() {
        const contentDiv = document.getElementById('reservoirTabContent');
        if (contentDiv && this._allSavedMappings) {
            // Temporarily show all mappings (user requested)
            const savedMappings = this._allSavedMappings;
        
        // Group mappings by reservoir
        const groupedMappings = {};
        savedMappings.forEach(([fieldName, reservoir]) => {
            if (!groupedMappings[reservoir]) {
                groupedMappings[reservoir] = [];
            }
            groupedMappings[reservoir].push(fieldName);
        });
        
        // Sort reservoirs alphabetically
        const sortedReservoirs = Object.keys(groupedMappings).sort();
        
        contentDiv.innerHTML = `
            <div style="display: grid; gap: 1.5rem;">
                    <div style="text-align: center; padding: 1rem; background: var(--warning-bg); border: 1px solid var(--warning-color); border-radius: 0.5rem; color: var(--warning-text);">
                        ⚠️ Showing all ${savedMappings.length} mappings - this may impact performance
                    </div>
                ${sortedReservoirs.map(reservoir => `
                    <div style="border: 1px solid var(--border-color); border-radius: 0.75rem; overflow: hidden; background: var(--surface-color);">
                        <div style="padding: 1rem; background: var(--gradient-primary); color: white;">
                            <h3 style="margin: 0; font-size: 1rem; font-weight: 600; display: flex; align-items: center; gap: 0.5rem;">
                                <span class="reservoir-display" style="background: rgba(255,255,255,0.2); padding: 0.25rem 0.5rem; border-radius: 0.25rem; font-size: 0.75rem;">
                                    ${reservoir}
                                </span>
                                <span style="font-size: 0.875rem; opacity: 0.9;">
                                    (${groupedMappings[reservoir].length} field${groupedMappings[reservoir].length !== 1 ? 's' : ''})
                                </span>
                            </h3>
                        </div>
                        <div style="padding: 1rem;">
                            <div style="display: grid; gap: 0.75rem;">
                                ${groupedMappings[reservoir].map(fieldName => `
                                    <div style="padding: 0.75rem; border: 1px solid var(--border-color); border-radius: 0.5rem; background: var(--background-color); display: flex; justify-content: space-between; align-items: center;">
                                        <div style="flex: 1; min-width: 0;">
                                            <div style="font-weight: 500; font-size: 0.875rem; color: var(--text-primary); margin-bottom: 0.25rem;">
                                                ${fieldName}
                                            </div>
                                            <div style="font-size: 0.75rem; color: var(--text-secondary);">
                                                Maps to: <strong>${reservoir}</strong>
                                            </div>
                                        </div>
                                        <div style="display: flex; gap: 0.5rem; margin-left: 1rem;">
                                            <button onclick="window.dashboard.editReservoirMapping('${fieldName.replace(/'/g, "\\'")}', '${reservoir.replace(/'/g, "\\'")}')" 
                                                    style="padding: 0.375rem 0.75rem; background: var(--gradient-primary); color: white; border: none; border-radius: 0.25rem; cursor: pointer; font-size: 0.75rem;">
                                                ✏️ Edit
                                            </button>
                                                <button onclick="window.dashboard.flagIncorrectFieldName('${fieldName.replace(/'/g, "\\'")}')" 
                                                        style="padding: 0.375rem 0.75rem; background: #ff6b35; color: white; border: none; border-radius: 0.25rem; cursor: pointer; font-size: 0.75rem;" 
                                                        title="Report this field name as incorrectly parsed">
                                                    🚩 Report Incorrect
                                            </button>
                                            <button onclick="window.dashboard.moveToReview('${fieldName.replace(/'/g, "\\'")}', '${reservoir.replace(/'/g, "\\'")}')" 
                                                    style="padding: 0.375rem 0.75rem; background: var(--gradient-accent); color: white; border: none; border-radius: 0.25rem; cursor: pointer; font-size: 0.75rem;">
                                                📋 Review
                                            </button>
                                            <button onclick="window.dashboard.deleteReservoirMapping('${fieldName.replace(/'/g, "\\'")}')" 
                                                    style="padding: 0.375rem 0.75rem; background: var(--error-color); color: white; border: none; border-radius: 0.25rem; cursor: pointer; font-size: 0.75rem;">
                                                🗑️
                                            </button>
                                        </div>
                                    </div>
                                `).join('')}
                            </div>
                        </div>
                    </div>
                `).join('')}
            </div>
        `;
        }
    }
    
    loadReviewQueueContent(contentDiv) {
        if (this.reviewQueue.length === 0) {
            contentDiv.innerHTML = `
                <div style="text-align: center; padding: 3rem; color: var(--text-secondary);">
                    <h3>No Items Under Review</h3>
                    <p>Field names you choose to review later will appear here</p>
                </div>
            `;
            return;
        }
        
        // PERFORMANCE FIX: Limit initial display to prevent 1000ms+ delays
        const REVIEW_DISPLAY_LIMIT = 10;
        
        // Flatten permits from review queue for individual display
        const individualPermits = [];
        this.reviewQueue.forEach(item => {
            item.permits.forEach(permit => {
                individualPermits.push({
                    ...permit,
                    currentFieldName: item.fieldName,
                    suggestedReservoir: item.suggestedReservoir,
                    addedAt: item.addedAt
                });
            });
        });
        
        const showAll = individualPermits.length <= REVIEW_DISPLAY_LIMIT;
        const displayPermits = showAll ? individualPermits : individualPermits.slice(0, REVIEW_DISPLAY_LIMIT);
        
        contentDiv.innerHTML = `
            <div style="display: grid; gap: 1rem;">
                ${displayPermits.map(permit => `
                    <div style="padding: 1rem; border: 1px solid var(--border-color); border-radius: 0.5rem; background: var(--background-color);">
                        <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 0.75rem;">
                            <div style="flex: 1; min-width: 0;">
                                <div style="font-weight: 600; font-size: 0.875rem; color: var(--text-primary); margin-bottom: 0.25rem;">
                                    ${permit.lease_name || 'Unknown Lease'}
                                </div>
                                <div style="font-size: 0.75rem; color: var(--text-secondary); margin-bottom: 0.25rem;">
                                    Status: ${permit.status_no} • ${permit.status_date ? new Date(permit.status_date).toLocaleDateString() : 'No date'}
                                </div>
                                <div style="font-size: 0.75rem; color: var(--text-secondary); margin-bottom: 0.5rem;">
                                    Current Field: <span style="color: var(--primary-color); font-weight: 500;">${permit.currentFieldName}</span>
                                </div>
                                <div style="font-size: 0.75rem; color: var(--text-secondary); margin-bottom: 0.75rem;">
                                    Suggested: <span style="color: #10b981; font-weight: 500;">${permit.suggestedReservoir}</span>
                                </div>
                                
                                ${permit.detail_url ? `
                                    <a href="${permit.detail_url}" target="_blank" 
                                       style="display: inline-block; padding: 0.375rem 0.75rem; background: var(--primary-color); color: white; text-decoration: none; border-radius: 0.375rem; font-size: 0.75rem; margin-bottom: 0.75rem;">
                                        📄 View Permit
                                    </a>
                                ` : `
                                    <span style="display: inline-block; padding: 0.375rem 0.75rem; background: #9ca3af; color: white; border-radius: 0.375rem; font-size: 0.75rem; margin-bottom: 0.75rem;">
                                        📄 No URL Available
                                    </span>
                                `}
                            </div>
                        </div>
                        <div style="display: flex; gap: 0.5rem; flex-wrap: wrap;">
                            <button onclick="window.dashboard.acceptCorrectReservoir('${permit.currentFieldName.replace(/'/g, "\\'")}', '${permit.suggestedReservoir.replace(/'/g, "\\'")}', '${permit.status_no}')" 
                                    style="padding: 0.5rem 0.75rem; background: #10b981; color: white; border: none; border-radius: 0.375rem; cursor: pointer; font-size: 0.75rem; white-space: nowrap;">
                                ✅ Accept as Correct
                            </button>
                            <button onclick="window.dashboard.correctSinglePermit(${JSON.stringify(permit).replace(/"/g, '&quot;')})" 
                                    style="padding: 0.5rem 0.75rem; background: #f59e0b; color: white; border: none; border-radius: 0.375rem; cursor: pointer; font-size: 0.75rem; white-space: nowrap;">
                                🎯 Correct Reservoir
                            </button>
                            <button onclick="window.dashboard.openManualMappingForPermit(${JSON.stringify(permit).replace(/"/g, '&quot;')})" 
                                    style="padding: 0.5rem 0.75rem; background: #3b82f6; color: white; border: none; border-radius: 0.375rem; cursor: pointer; font-size: 0.75rem; white-space: nowrap;">
                                ➕ Manual Mapping
                            </button>
                            <button onclick="window.dashboard.getSinglePermitSuggestion(${JSON.stringify(permit).replace(/"/g, '&quot;')})" 
                                    style="padding: 0.5rem 0.75rem; background: #8b5cf6; color: white; border: none; border-radius: 0.375rem; cursor: pointer; font-size: 0.75rem; white-space: nowrap;">
                                🤖 AI Suggest
                            </button>
                            <button onclick="window.dashboard.removeSinglePermitFromReview('${permit.currentFieldName.replace(/'/g, "\\'")}', '${permit.status_no}')" 
                                    style="padding: 0.5rem 0.75rem; background: var(--error-color); color: white; border: none; border-radius: 0.375rem; cursor: pointer; font-size: 0.75rem;">
                                ✕ Remove
                            </button>
                        </div>
                    </div>
                `).join('')}
                ${!showAll ? `
                    <div style="text-align: center; padding: 2rem; border-top: 1px solid var(--border-color); margin-top: 1rem;">
                        <p style="color: var(--text-secondary); margin-bottom: 1rem;">
                            Showing ${displayPermits.length} of ${individualPermits.length} review items
                        </p>
                        <button onclick="window.dashboard.loadAllReviewItems()" 
                                style="padding: 0.75rem 1.5rem; background: var(--gradient-primary); color: white; border: none; border-radius: 0.375rem; cursor: pointer; font-size: 0.875rem;">
                            📋 Show All ${individualPermits.length} Items
                        </button>
                    </div>
                ` : ''}
            </div>
        `;
        
        // Store reference for "Show All" functionality
        this._allReviewItems = individualPermits;
    }
    
    loadAllReviewItems() {
        const contentDiv = document.getElementById('reservoirTabContent');
        if (contentDiv && this._allReviewItems) {
            // Show warning and render all items
            const allItems = this._allReviewItems;
            
            contentDiv.innerHTML = `
                <div style="display: grid; gap: 1rem;">
                    <div style="text-align: center; padding: 1rem; background: var(--warning-bg); border: 1px solid var(--warning-color); border-radius: 0.5rem; color: var(--warning-text);">
                        ⚠️ Showing all ${allItems.length} review items - this may impact performance
                    </div>
                    ${allItems.map(permit => `
                        <div style="padding: 1rem; border: 1px solid var(--border-color); border-radius: 0.5rem; background: var(--background-color);">
                            <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 0.75rem;">
                                <div style="flex: 1; min-width: 0;">
                                    <div style="font-weight: 600; font-size: 0.875rem; color: var(--text-primary); margin-bottom: 0.25rem;">
                                        ${permit.lease_name || 'Unknown Lease'}
                                    </div>
                                    <div style="font-size: 0.75rem; color: var(--text-secondary); margin-bottom: 0.25rem;">
                                        Status: ${permit.status_no} • ${permit.status_date ? new Date(permit.status_date).toLocaleDateString() : 'No date'}
                                    </div>
                                    <div style="font-size: 0.75rem; color: var(--text-secondary); margin-bottom: 0.5rem;">
                                        Current Field: <span style="color: var(--primary-color); font-weight: 500;">${permit.currentFieldName}</span>
                                    </div>
                                    <div style="font-size: 0.75rem; color: var(--text-secondary); margin-bottom: 0.75rem;">
                                        Suggested: <span style="color: #10b981; font-weight: 500;">${permit.suggestedReservoir}</span>
                                    </div>
                                </div>
                            </div>
                            <div style="display: flex; gap: 0.5rem; flex-wrap: wrap;">
                                <button onclick="window.dashboard.acceptCorrectReservoir('${permit.currentFieldName.replace(/'/g, "\\'")}', '${permit.suggestedReservoir.replace(/'/g, "\\'")}', '${permit.status_no}')" 
                                        style="padding: 0.5rem 0.75rem; background: #10b981; color: white; border: none; border-radius: 0.375rem; cursor: pointer; font-size: 0.75rem; white-space: nowrap;">
                                    ✅ Accept as Correct
                                </button>
                                <button onclick="window.dashboard.removeSinglePermitFromReview('${permit.currentFieldName.replace(/'/g, "\\'")}', '${permit.status_no}')" 
                                        style="padding: 0.5rem 0.75rem; background: var(--error-color); color: white; border: none; border-radius: 0.375rem; cursor: pointer; font-size: 0.75rem;">
                                    ✕ Remove
                                </button>
                            </div>
                        </div>
                    `).join('')}
                </div>
            `;
        }
    }
    
    loadFlaggedPermitsContent(contentDiv) {
        // Get flagged field names from localStorage
        const flaggedFieldNames = JSON.parse(localStorage.getItem('flaggedFieldNames') || '[]');
        
        // Update the flagged count in the tab
        const flaggedCount = document.getElementById('flaggedCount');
        if (flaggedCount) {
            flaggedCount.textContent = flaggedFieldNames.length;
        }
        
        if (flaggedFieldNames.length === 0) {
            contentDiv.innerHTML = `
                <div style="text-align: center; padding: 3rem; color: var(--text-secondary);">
                    <div style="font-size: 3rem; margin-bottom: 1rem;">✅</div>
                    <h3>No Flagged Field Names</h3>
                    <p>Field names flagged for incorrect parsing will appear here</p>
                    <p style="font-size: 0.875rem; margin-top: 1rem;">
                        Use the <strong>🚩 Report Incorrect</strong> button in the Saved Mappings tab to flag field names with wrong parsing.
                    </p>
                </div>
            `;
            return;
        }
        
        contentDiv.innerHTML = `
            <div style="margin-bottom: 1.5rem; padding: 1rem; background: linear-gradient(135deg, #ff6b35, #f7931e); border-radius: 0.75rem; color: white;">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <h3 style="margin: 0 0 0.5rem 0; font-size: 1.1rem;">🚩 ${flaggedFieldNames.length} Field Name${flaggedFieldNames.length !== 1 ? 's' : ''} Flagged for Re-processing</h3>
                        <p style="margin: 0; font-size: 0.875rem; opacity: 0.9;">
                            These field names have incorrect parsing and need manual correction in the parser.
                        </p>
                    </div>
                    <button onclick="window.dashboard.clearAllFlagged()" 
                            style="padding: 0.75rem 1.5rem; background: rgba(255,255,255,0.2); color: white; border: 2px solid white; border-radius: 0.5rem; cursor: pointer; font-weight: 600; font-size: 0.875rem;">
                        🗑️ Clear All
                    </button>
                </div>
            </div>
            
            <div style="display: grid; gap: 1rem;">
                ${flaggedFieldNames.map(item => `
                    <div style="padding: 1rem; border: 2px solid #ff6b35; border-radius: 0.75rem; background: var(--background-color);">
                        <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 0.75rem;">
                            <div style="flex: 1; min-width: 0;">
                                <div style="display: flex; align-items: center; gap: 0.75rem; margin-bottom: 0.5rem;">
                                    <span style="background: #ff6b35; color: white; padding: 0.25rem 0.5rem; border-radius: 0.25rem; font-size: 0.75rem; font-weight: 600;">
                                        FLAGGED
                                    </span>
                                    <span style="font-size: 0.75rem; color: var(--text-secondary);">
                                        Flagged ${new Date(item.flaggedAt).toLocaleDateString()}
                                    </span>
                                </div>
                                <div style="background: #fff3f0; border: 1px solid #ff6b35; border-radius: 0.375rem; padding: 0.5rem; margin-bottom: 0.75rem;">
                                    <div style="font-size: 0.75rem; color: #d63031; font-weight: 600; margin-bottom: 0.25rem;">❌ Incorrect Field Name:</div>
                                    <div style="font-size: 0.875rem; color: var(--text-primary); font-family: monospace; word-break: break-all;">
                                        "${item.fieldName}"
                                    </div>
                                </div>
                                <div style="font-size: 0.75rem; color: var(--text-secondary);">
                                    <strong>Note:</strong> This field name needs to be fixed in the parser code. 
                                    The improved parsing logic should handle cases like this better.
                                </div>
                            </div>
                        </div>
                        <div style="display: flex; gap: 0.5rem;">
                            <button onclick="window.dashboard.unflagFieldName('${item.fieldName.replace(/'/g, "\\'")}', '${item.flaggedAt}')" 
                                    style="padding: 0.5rem; background: var(--text-secondary); color: white; border: none; border-radius: 0.375rem; cursor: pointer; font-size: 0.75rem;">
                                ✕ Remove Flag
                            </button>
                        </div>
                    </div>
                `).join('')}
            </div>
        `;
    }
    
    loadCancelledMappingsContent(contentDiv) {
        if (this.cancelledMappings.length === 0) {
            contentDiv.innerHTML = `
                <div style="text-align: center; padding: 3rem; color: var(--text-secondary);">
                    <h3>No Cancelled Mappings</h3>
                    <p>Field names you cancel will appear here for later review</p>
                </div>
            `;
            return;
        }
        
        contentDiv.innerHTML = `
            <div style="display: grid; gap: 1rem;">
                ${this.cancelledMappings.map(item => `
                    <div style="padding: 1rem; border: 1px solid var(--border-color); border-radius: 0.5rem; background: var(--background-color);">
                        <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 0.75rem;">
                            <div style="flex: 1; min-width: 0;">
                                <div style="font-weight: 600; font-size: 0.875rem; color: var(--primary-color); margin-bottom: 0.25rem;">
                                    ${item.fieldName}
                                </div>
                                <div style="font-size: 0.75rem; color: var(--text-secondary); margin-bottom: 0.5rem;">
                                    Suggested: ${item.suggestedReservoir}
                                </div>
                                <div style="font-size: 0.75rem; color: var(--text-secondary);">
                                    ${item.permits.length} permit${item.permits.length !== 1 ? 's' : ''} • Cancelled ${new Date(item.cancelledAt).toLocaleDateString()}
                                </div>
                            </div>
                        </div>
                        <div style="display: flex; gap: 0.5rem;">
                            <button onclick="window.dashboard.reviewCancelledMapping('${item.fieldName.replace(/'/g, "\\'")}', '${item.suggestedReservoir.replace(/'/g, "\\'")}')" 
                                    style="flex: 1; padding: 0.5rem; background: var(--gradient-primary); color: white; border: none; border-radius: 0.375rem; cursor: pointer; font-size: 0.75rem;">
                                🔄 Review Again
                            </button>
                            <button onclick="window.dashboard.moveToReview('${item.fieldName.replace(/'/g, "\\'")}', '${item.suggestedReservoir.replace(/'/g, "\\'")}')" 
                                    style="padding: 0.5rem; background: var(--gradient-accent); color: white; border: none; border-radius: 0.375rem; cursor: pointer; font-size: 0.75rem;">
                                📋 Review
                            </button>
                            <button onclick="window.dashboard.removeFromCancelledMappings('${item.fieldName.replace(/'/g, "\\'")}')" 
                                    style="padding: 0.5rem; background: var(--error-color); color: white; border: none; border-radius: 0.375rem; cursor: pointer; font-size: 0.75rem;">
                                🗑️
                            </button>
                        </div>
                    </div>
                `).join('')}
            </div>
        `;
    }
    
    editReservoirMapping(fieldName, currentReservoir) {
        const newReservoir = prompt(`Edit reservoir mapping for "${fieldName}":`, currentReservoir);
        if (newReservoir && newReservoir.trim() !== currentReservoir) {
            this.reservoirMapping[fieldName] = newReservoir.trim();
            localStorage.setItem('reservoirMapping', JSON.stringify(this.reservoirMapping));
            this.renderPermitCards();
            this.switchReservoirTab('saved'); // Refresh the display
            this.showSuccess(`Updated mapping: "${fieldName}" → "${newReservoir.trim()}"`);
        }
    }
    
    deleteReservoirMapping(fieldName) {
        if (confirm(`Delete reservoir mapping for "${fieldName}"?`)) {
            delete this.reservoirMapping[fieldName];
            localStorage.setItem('reservoirMapping', JSON.stringify(this.reservoirMapping));
            this.renderPermitCards();
            this.switchReservoirTab('saved'); // Refresh the display
            this.showSuccess(`Deleted mapping for "${fieldName}"`);
        }
    }
    
    reviewCancelledMapping(fieldName, suggestedReservoir) {
        // Remove from cancelled and show the review modal
        this.removeFromCancelledMappings(fieldName);
        const relatedPermits = this.getPermitsWithFieldName(fieldName);
        this.showReservoirReviewModal(fieldName, suggestedReservoir, relatedPermits);
    }
    
    moveToReview(fieldName, suggestedReservoir) {
        // Remove from saved mappings if it exists there
        if (this.reservoirMapping[fieldName]) {
            delete this.reservoirMapping[fieldName];
            localStorage.setItem('reservoirMapping', JSON.stringify(this.reservoirMapping));
        }
        
        // Remove from cancelled mappings if it exists there
        this.removeFromCancelledMappings(fieldName);
        
        // Add to review queue if not already there
        const existingIndex = this.reviewQueue.findIndex(item => item.fieldName === fieldName);
        if (existingIndex === -1) {
            this.reviewQueue.push({
                fieldName: fieldName,
                suggestedReservoir: suggestedReservoir,
                addedAt: new Date().toISOString(),
                permits: this.getPermitsWithFieldName(fieldName)
            });
            
            localStorage.setItem('reservoirReviewQueue', JSON.stringify(this.reviewQueue));
        }
        
        // Update display
        this.updateReviewQueueDisplay();
        this.renderPermitCards(); // Re-render cards to reflect changes
        
        // Refresh the current tab display
        const activeTab = document.querySelector('.reservoir-tab.active');
        if (activeTab) {
            const tabName = activeTab.id.replace('Tab', '');
            this.switchReservoirTab(tabName);
        }
        
        this.showSuccess(`"${fieldName}" moved to Under Review queue`);
    }
    
    exportReservoirMappings() {
        const exportData = {
            savedMappings: this.reservoirMapping,
            reviewQueue: this.reviewQueue,
            cancelledMappings: this.cancelledMappings,
            exportedAt: new Date().toISOString()
        };
        
        const dataStr = JSON.stringify(exportData, null, 2);
        const dataBlob = new Blob([dataStr], {type: 'application/json'});
        const url = URL.createObjectURL(dataBlob);
        
        const link = document.createElement('a');
        link.href = url;
        link.download = `reservoir-mappings-${new Date().toISOString().split('T')[0]}.json`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
        
        this.showSuccess('Reservoir mappings exported successfully!');
    }

    flagIncorrectFieldName(fieldName) {
        try {
            // Confirm with user
            const confirmed = confirm(
                `Flag field name "${fieldName}" as incorrectly parsed?\n\n` +
                `This will add it to your flagged list for tracking. The field name needs to be fixed in the parser code to handle cases like this better.`
            );
            
            if (!confirmed) return;
            
            // Get existing flagged field names
            const flaggedFieldNames = JSON.parse(localStorage.getItem('flaggedFieldNames') || '[]');
            
            // Check if already flagged
            const alreadyFlagged = flaggedFieldNames.some(item => item.fieldName === fieldName);
            if (alreadyFlagged) {
                this.showError(`Field name "${fieldName}" is already flagged`);
                return;
            }
            
            // Add to flagged list
            flaggedFieldNames.push({
                fieldName: fieldName,
                flaggedAt: new Date().toISOString(),
                status: 'pending'
            });
            
            // Save to localStorage
            localStorage.setItem('flaggedFieldNames', JSON.stringify(flaggedFieldNames));
            
            this.showSuccess(`✅ Flagged field name "${fieldName}" for tracking`);
            
            // Switch to flagged tab to show the results
            this.switchReservoirTab('flagged');
            
        } catch (error) {
            console.error('Error flagging field name:', error);
            this.showError(`Error: ${error.message}`);
        }
    }

    async flagForReenrich(statusNo) {
        try {
            // Find the specific permit
            const permit = this.permits.find(p => p.status_no === statusNo);
            if (!permit) {
                alert(`Permit ${statusNo} not found`);
                return;
            }

            // Confirm with user
            const confirmed = confirm(
                `Re-enrich permit ${statusNo}?\n\n` +
                `Lease: ${permit.lease_name || 'Unknown'}\n` +
                `Operator: ${permit.operator_name || 'Unknown'}\n\n` +
                `This will re-extract detailed information from the RRC detail page for this specific permit.`
            );
            
            if (!confirmed) return;

            // Show loading state
            console.log(`🔄 Re-enriching permit ${statusNo}...`);

            // Call the backend API to re-enrich this specific permit
            const response = await fetch(`/api/v1/permits/${statusNo}/re-enrich`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Cache-Control': 'no-cache'
                }
            });

            if (!response.ok) {
                let errorMessage = `HTTP ${response.status}: ${response.statusText}`;
                try {
                    const errorData = await response.json();
                    if (errorData.detail) {
                        errorMessage = errorData.detail;
                    }
                } catch (e) {
                    // Use basic error message if can't parse JSON
                }
                throw new Error(errorMessage);
            }

            const result = await response.json();

            // Show success message
            const successMsg = document.createElement('div');
            successMsg.style.cssText = 'position: fixed; top: 20px; right: 20px; background: #10b981; color: white; padding: 12px 20px; border-radius: 8px; z-index: 1002; font-weight: 500;';
            successMsg.textContent = `✅ Permit ${statusNo} re-enriched successfully`;
            document.body.appendChild(successMsg);

            // Auto-remove success message after 4 seconds
            setTimeout(() => {
                if (successMsg.parentNode) {
                    successMsg.remove();
                }
            }, 4000);

            // Refresh the permit data to show updated information
            setTimeout(() => {
                this.loadPermits();
            }, 1500);
            
        } catch (error) {
            console.error('Error re-enriching permit:', error);
            alert(`Error re-enriching permit: ${error.message}`);
        }
    }

    clearAllFlagged() {
        try {
            const confirmed = confirm(
                '🗑️ Clear all flagged field names?\n\n' +
                'This will remove all flagged items from your tracking list.'
            );
            
            if (!confirmed) return;
            
            // Clear localStorage
            localStorage.setItem('flaggedFieldNames', JSON.stringify([]));
            
            this.showSuccess('✅ Cleared all flagged field names');
            
            // Refresh the flagged tab
            this.switchReservoirTab('flagged');
            
        } catch (error) {
            console.error('Error clearing flagged items:', error);
            this.showError(`❌ Failed to clear flagged items: ${error.message}`);
        }
    }

    unflagFieldName(fieldName, flaggedAt) {
        try {
            const confirmed = confirm(
                `Remove "${fieldName}" from flagged list?\n\n` +
                'This will remove it from your tracking list.'
            );
            
            if (!confirmed) return;
            
            // Get existing flagged field names
            const flaggedFieldNames = JSON.parse(localStorage.getItem('flaggedFieldNames') || '[]');
            
            // Remove the specific item
            const updatedFlagged = flaggedFieldNames.filter(item => 
                !(item.fieldName === fieldName && item.flaggedAt === flaggedAt)
            );
            
            // Save back to localStorage
            localStorage.setItem('flaggedFieldNames', JSON.stringify(updatedFlagged));
            
            this.showSuccess(`✅ Removed "${fieldName}" from flagged list`);
            
            // Refresh the flagged tab
            this.switchReservoirTab('flagged');
            
        } catch (error) {
            console.error('Error removing flagged item:', error);
            this.showError(`❌ Failed to remove flagged item: ${error.message}`);
        }
    }
    
    showPermitDetails(permit) {
        // Create modal or side panel with permit details
        const modal = document.createElement('div');
        modal.className = 'fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50';
        modal.innerHTML = `
            <div class="bg-white rounded-lg p-6 max-w-2xl w-full mx-4 max-h-90vh overflow-y-auto">
                <div class="flex justify-between items-center mb-4">
                    <h2 class="text-xl font-bold">Permit Details</h2>
                    <button class="close-modal text-gray-500 hover:text-gray-700 text-2xl">&times;</button>
                </div>
                <div class="grid grid-cols-2 gap-4 text-sm">
                    <div><strong>Status No:</strong> ${permit.status_no || '-'}</div>
                    <div><strong>API No:</strong> ${permit.api_no || '-'}</div>
                    <div><strong>Status Date:</strong> ${this.formatDate(permit.status_date)}</div>
                    <div><strong>Operator:</strong> ${permit.operator_name || '-'}</div>
                    <div><strong>Lease Name:</strong> ${permit.lease_name || '-'}</div>
                    <div><strong>Well No:</strong> ${permit.well_no || '-'}</div>
                    <div><strong>County:</strong> ${permit.county || '-'}</div>
                    <div><strong>District:</strong> ${permit.district || '-'}</div>
                    <div><strong>Filing Purpose:</strong> ${permit.filing_purpose || '-'}</div>
                    <div><strong>Current Queue:</strong> ${permit.current_queue || '-'}</div>
                    <div><strong>Field Name:</strong> ${permit.field_name || '-'}</div>
                    <div><strong>Acres:</strong> ${permit.acres || '-'}</div>
                    <div><strong>Section:</strong> ${permit.section || '-'}</div>
                    <div><strong>Block:</strong> ${permit.block || '-'}</div>
                    <div><strong>Survey:</strong> ${permit.survey || '-'}</div>
                    <div><strong>Abstract:</strong> ${permit.abstract_no || '-'}</div>
                    <div><strong>Total Depth:</strong> ${permit.total_depth || '-'}</div>
                    <div><strong>Wellbore Profile:</strong> ${permit.wellbore_profile || '-'}</div>
                </div>
                <div class="mt-6 flex gap-2">
                    ${permit.detail_url ? `
                        <a href="${permit.detail_url}" target="_blank" class="btn btn-primary">
                            View RRC Details
                        </a>
                    ` : ''}
                    ${permit.w1_pdf_url ? `
                        <a href="${permit.w1_pdf_url}" target="_blank" class="btn btn-secondary">
                            View W-1 PDF
                        </a>
                    ` : ''}
                </div>
            </div>
        `;
        
        modal.querySelector('.close-modal').addEventListener('click', () => {
            modal.remove();
        });
        
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                modal.remove();
            }
        });
        
        document.body.appendChild(modal);
    }
    
    getStatusClass(purpose) {
        if (!purpose) return '';
        const p = purpose.toLowerCase();
        if (p.includes('new')) return 'status-new';
        if (p.includes('amend')) return 'status-amendment';
        return '';
    }
    
    getQueueClass(queue) {
        if (!queue) return '';
        const q = queue.toLowerCase();
        if (q.includes('mapping')) return 'status-mapping';
        if (q.includes('review')) return 'status-review';
        return '';
    }
    
    formatDate(dateStr) {
        if (!dateStr) return '-';
        try {
            // Handle various date formats
            let date;
            
            // Handle ISO format from created_at (2025-09-29T15:53:56:948Z)
            if (dateStr.includes('T')) {
                date = new Date(dateStr);
            }
            // Handle MM-DD-YYYY format from status_date
            else if (dateStr.includes('-') && dateStr.length === 10) {
                const parts = dateStr.split('-');
                if (parts.length === 3 && parts[0].length === 2) {
                    // Assume MM-DD-YYYY format
                    const [month, day, year] = parts;
                    date = new Date(`${year}-${month}-${day}`);
                } else {
                    date = new Date(dateStr);
                }
            } else {
                date = new Date(dateStr);
            }
            
            if (isNaN(date.getTime())) {
                return dateStr; // Return original if parsing fails
            }
            
            return date.toLocaleDateString('en-US', {
                month: 'short',
                day: 'numeric',
                year: 'numeric'
            });
        } catch {
            return dateStr;
        }
    }
    
    updateStats() {
        // Filter dismissed permits for "Permits Visible" count
        const activePermits = this.permits.filter(p => !this.dismissedPermits.has(p.status_no));
        const visiblePermits = activePermits.length; // Only count non-dismissed permits
        
        const todayPermits = activePermits.filter(p => this.isPermitFromToday(p)).length;
        
        const newDrillPermits = activePermits.filter(p => 
            (p.filing_purpose || '').toLowerCase().includes('new')
        ).length;
        
        const amendmentPermits = activePermits.filter(p => 
            (p.filing_purpose || '').toLowerCase().includes('amend')
        ).length;
        
        // Update header stats
        document.getElementById('totalPermits').textContent = visiblePermits;
        document.getElementById('todayPermits').textContent = todayPermits;
        document.getElementById('newDrillCount').textContent = newDrillPermits;
        document.getElementById('amendmentCount').textContent = amendmentPermits;
        
        // Update sidebar stats
        const totalPermits2El = document.getElementById('totalPermits2');
        const newPermitsTodayEl = document.getElementById('newPermitsToday');
        const amendmentsTodayEl = document.getElementById('amendmentsToday');
        const uniqueOperatorsEl = document.getElementById('uniqueOperators');
        
        if (totalPermits2El) totalPermits2El.textContent = visiblePermits;
        if (newPermitsTodayEl) newPermitsTodayEl.textContent = todayPermits;
        if (amendmentsTodayEl) amendmentsTodayEl.textContent = amendmentPermits;
        
        // Calculate unique operators
        const uniqueOperators = new Set(
            this.permits
                .map(p => p.operator_name)
                .filter(name => name && name.trim())
        ).size;
        if (uniqueOperatorsEl) uniqueOperatorsEl.textContent = uniqueOperators;
        
        // Update top operators and reservoirs
        this.updateTopOperators();
        this.updateTopReservoirs();
        this.updateParsingStats();
    }
    
    updateFilterStats() {
        document.getElementById('filteredCount').textContent = this.filteredPermits.length;
    }
    
    updateTopOperators() {
        const topOperatorsEl = document.getElementById('topOperators');
        if (!topOperatorsEl) return;
        
        // Count permits by operator (include dismissed permits for trend analysis)
        const operatorCounts = {};
        this.permits.forEach(permit => {
            const operatorName = this.cleanOperatorName(permit.operator_name);
            if (operatorName) {
                operatorCounts[operatorName] = (operatorCounts[operatorName] || 0) + 1;
            }
        });
        
        // Sort by count and get top 5
        const topOperators = Object.entries(operatorCounts)
            .sort(([,a], [,b]) => b - a)
            .slice(0, 5);
        
        if (topOperators.length === 0) {
            topOperatorsEl.innerHTML = '<div style="color: var(--text-secondary); text-align: center; padding: 1rem;">No operators found</div>';
            return;
        }
        
        topOperatorsEl.innerHTML = topOperators.map(([operator, count]) => `
            <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.5rem 0; border-bottom: 1px solid var(--border-color);">
                <span style="font-size: 0.875rem; font-weight: 500; color: var(--text-primary);">${operator}</span>
                <span style="font-size: 0.875rem; font-weight: 600; color: var(--primary-color);">${count}</span>
            </div>
        `).join('');
    }
    
    cleanOperatorName(operatorName) {
        if (!operatorName) return null;
        
        // Remove operator number in parentheses (e.g., "COMPANY NAME (123456)" -> "COMPANY NAME")
        return operatorName.replace(/\s*\([^)]*\)\s*$/, '').trim();
    }
    
    extractReservoir(fieldName) {
        if (!fieldName) return 'UNKNOWN';
        
        // Check if we have a mapping for this field name
        if (this.reservoirMapping[fieldName]) {
            return this.reservoirMapping[fieldName];
        }
        
        // Smart extraction patterns
        const patterns = [
            // Pattern: "FIELD NAME (RESERVOIR NAME)"
            /\(([^)]+)\)$/,
            // Pattern: "RESERVOIR NAME (ADDITIONAL INFO)"  
            /^([A-Z\s]+)\s*\(/,
            // Pattern: Just the field name if no parentheses
            /^([A-Z\s]+)$/
        ];
        
        for (const pattern of patterns) {
            const match = fieldName.match(pattern);
            if (match) {
                let reservoir = match[1].trim();
                
                // Clean up common reservoir names
                reservoir = reservoir
                    .replace(/\bTREND\s+AREA\b/g, '')
                    .replace(/\bSHALE\b/g, 'SHALE')
                    .replace(/\bFORD\b/g, 'FORD')
                    .replace(/\bCHALK\b/g, 'CHALK')
                    .replace(/\bCAMP\b/g, 'CAMP')
                    .trim();
                
                // If this is a new reservoir, add it to our mapping and alert user
                if (!this.reservoirMapping[fieldName]) {
                    this.handleNewReservoir(fieldName, reservoir);
                }
                
                return reservoir;
            }
        }
        
        // Fallback - use the field name and alert for manual mapping
        this.handleNewReservoir(fieldName, fieldName);
        return fieldName;
    }
    
    handleNewReservoir(fieldName, suggestedReservoir) {
        // Check if we've already alerted for this field
        const alertKey = `reservoir_alert_${fieldName}`;
        if (localStorage.getItem(alertKey)) {
            return; // Already handled
        }
        
        // Mark as alerted
        localStorage.setItem(alertKey, 'true');
        
        // Create alert modal for new reservoir mapping
        this.showReservoirMappingModal(fieldName, suggestedReservoir);
    }
    
    showReservoirMappingModal(fieldName, suggestedReservoir) {
        const modal = document.createElement('div');
        modal.className = 'fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50';
        modal.style.cssText = 'position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.5); display: flex; align-items: center; justify-content: center; z-index: 1000;';
        
        modal.innerHTML = `
            <div style="background: white; padding: 2rem; border-radius: 0.75rem; max-width: 600px; margin: 1rem;">
                <h3 style="font-size: 1.25rem; font-weight: 600; margin-bottom: 1rem; color: var(--primary-color);">
                    🎯 New Reservoir Detected
                </h3>
                <p style="margin-bottom: 1rem; color: var(--text-secondary);">
                    Found a new field name: <strong>"${fieldName}"</strong>
                </p>
                <p style="margin-bottom: 1rem;">
                    What should be displayed as the reservoir name on cards?
                </p>
                <input type="text" id="reservoirInput" value="${suggestedReservoir}" 
                       style="width: 100%; padding: 0.75rem; border: 1px solid var(--border-color); border-radius: 0.5rem; margin-bottom: 1rem;">
                <div style="display: flex; gap: 0.75rem; justify-content: space-between; align-items: center;">
                    <button id="reviewReservoir" style="padding: 0.75rem 1rem; background: var(--gradient-accent); color: white; border: none; border-radius: 0.5rem; cursor: pointer;">
                        📋 Review Later
                    </button>
                    <div style="display: flex; gap: 0.75rem;">
                        <button id="cancelReservoir" style="padding: 0.75rem 1rem; background: var(--surface-color); border: 1px solid var(--border-color); border-radius: 0.5rem; cursor: pointer;">
                            Cancel
                        </button>
                        <button id="saveReservoir" style="padding: 0.75rem 1rem; background: var(--gradient-primary); color: white; border: none; border-radius: 0.5rem; cursor: pointer;">
                            Save Mapping
                        </button>
                    </div>
                </div>
            </div>
        `;
        
        document.body.appendChild(modal);
        
        // Event handlers
        modal.querySelector('#cancelReservoir').addEventListener('click', () => {
            this.addToCancelledMappings(fieldName, suggestedReservoir);
            modal.remove();
        });
        
        modal.querySelector('#reviewReservoir').addEventListener('click', () => {
            this.addToReviewQueue(fieldName, suggestedReservoir);
            this.showSuccess(`"${fieldName}" added to review queue. You can review it later from the sidebar.`);
            modal.remove();
        });
        
        modal.querySelector('#saveReservoir').addEventListener('click', () => {
            const reservoirName = modal.querySelector('#reservoirInput').value.trim();
            if (reservoirName) {
                this.reservoirMapping[fieldName] = reservoirName;
                localStorage.setItem('reservoirMapping', JSON.stringify(this.reservoirMapping));
                this.renderPermitCards(); // Re-render with new mapping
                this.showSuccess(`Reservoir mapping saved: "${fieldName}" → "${reservoirName}"`);
            }
            modal.remove();
        });
        
        // Focus the input
        setTimeout(() => {
            modal.querySelector('#reservoirInput').focus();
        }, 100);
    }
    
    buildMultiSelectOptions() {
        // Build operator options
        const operators = [...new Set(
            this.permits
                .map(p => this.cleanOperatorName(p.operator_name))
                .filter(name => name && name.trim())
        )].sort();
        
        const operatorOptions = document.getElementById('operatorOptions');
        operatorOptions.innerHTML = operators.map((operator, index) => `
            <div class="multi-select-option">
                <input type="checkbox" id="op_${index}" 
                       value="${operator}" onchange="window.dashboard.toggleOperatorSelection('${operator.replace(/'/g, "\\'")}')">
                <label for="op_${index}">${operator}</label>
            </div>
        `).join('');
        
        // Build county options
        const counties = [...new Set(
            this.permits
                .map(p => p.county)
                .filter(county => county && county.trim())
        )].sort();
        
        const countyOptions = document.getElementById('countyOptions');
        countyOptions.innerHTML = counties.map((county, index) => `
            <div class="multi-select-option">
                <input type="checkbox" id="co_${index}" 
                       value="${county}" onchange="window.dashboard.toggleCountySelection('${county.replace(/'/g, "\\'")}')">
                <label for="co_${index}">${county}</label>
            </div>
        `).join('');
    }
    
    toggleMultiSelect(elementId) {
        const element = document.getElementById(elementId);
        const isVisible = element.style.display !== 'none';
        
        // Hide all multi-selects first
        document.querySelectorAll('.multi-select-content').forEach(el => {
            el.style.display = 'none';
        });
        
        // Show this one if it was hidden
        if (!isVisible) {
            element.style.display = 'block';
        }
    }
    
    toggleOperatorSelection(operator) {
        if (this.selectedOperators.has(operator)) {
            this.selectedOperators.delete(operator);
        } else {
            this.selectedOperators.add(operator);
        }
        this.updateMultiSelectButton('operatorMultiBtn', this.selectedOperators, 'operators');
        this.applyFilters();
    }
    
    toggleCountySelection(county) {
        if (this.selectedCounties.has(county)) {
            this.selectedCounties.delete(county);
        } else {
            this.selectedCounties.add(county);
        }
        this.updateMultiSelectButton('countyMultiBtn', this.selectedCounties, 'counties');
        this.applyFilters();
    }
    
    updateMultiSelectButton(buttonId, selectedSet, type) {
        const button = document.getElementById(buttonId);
        const count = selectedSet.size;
        
        if (count === 0) {
            button.textContent = `📋 Select Multiple`;
        } else {
            button.textContent = `📋 ${count} ${type} selected`;
        }
    }
    
    async openReservoirTrends(specificReservoir = null) {
        try {
        // Create the modal
        const modal = document.createElement('div');
        modal.className = 'trends-modal';
        modal.style.cssText = `
            position: fixed; 
            top: 0; 
            left: 0; 
            right: 0; 
            bottom: 0; 
            background: rgba(0,0,0,0.7); 
            display: flex; 
            align-items: center; 
            justify-content: center; 
            z-index: 2000;
            animation: fadeIn 0.3s ease-out;
        `;
        
        const modalContent = document.createElement('div');
        modalContent.style.cssText = `
            background: white; 
            border-radius: 1rem; 
            width: 90vw; 
            height: 80vh; 
            max-width: 1200px; 
            position: relative;
            display: flex;
            flex-direction: column;
            box-shadow: 0 25px 50px -12px rgb(0 0 0 / 0.25);
        `;
        
        modalContent.innerHTML = `
            <div style="padding: 1.5rem; border-bottom: 1px solid var(--border-color); display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <h2 style="margin: 0; font-size: 1.5rem; font-weight: 600; color: var(--primary-color);">
                        📈 Reservoir Trends Analysis
                    </h2>
                    <p style="margin: 0.5rem 0 0 0; color: var(--text-secondary); font-size: 0.875rem;">
                        ${specificReservoir ? `Showing trends for ${specificReservoir}` : 'Historical permit activity by reservoir'}
                    </p>
                </div>
                <button onclick="this.closest('.trends-modal').remove()" 
                        style="padding: 0.5rem; background: none; border: none; font-size: 1.5rem; cursor: pointer; color: var(--text-secondary);">
                    ✕
                </button>
            </div>
            
            <div style="padding: 1rem; display: flex; gap: 1rem; border-bottom: 1px solid var(--border-color); align-items: center; flex-wrap: wrap;">
                <div style="display: flex; gap: 0.5rem; align-items: center;">
                    <label style="font-weight: 500; color: var(--text-primary);">Time Range:</label>
                    <select id="timeRangeSelect" style="padding: 0.5rem; border: 1px solid var(--border-color); border-radius: 0.375rem;">
                        <option value="7">Last 7 Days</option>
                        <option value="30">Last 30 Days</option>
                        <option value="90" selected>Last 90 Days</option>
                        <option value="180">Last 6 Months</option>
                        <option value="365">Last Year</option>
                    </select>
                </div>
                
                <div style="display: flex; gap: 0.5rem; align-items: center;">
                    <label style="font-weight: 500; color: var(--text-primary);">View:</label>
                    <select id="chartViewSelect" style="padding: 0.5rem; border: 1px solid var(--border-color); border-radius: 0.375rem; min-width: 150px;">
                        <option value="daily">Daily Trends</option>
                        <option value="cumulative">Cumulative Total</option>
                    </select>
                </div>
                
                <div style="display: flex; gap: 0.5rem; align-items: center;">
                    <button id="refreshChart" style="padding: 0.5rem 1rem; background: var(--gradient-primary); color: white; border: none; border-radius: 0.375rem; cursor: pointer;">
                        🔄 Refresh
                    </button>
                </div>
            </div>
            
            <div style="flex: 1; padding: 1rem; display: flex; gap: 1rem; min-height: 0;">
                <div style="flex: 1; min-width: 0; display: flex; flex-direction: column;">
                    <canvas id="reservoirChart" style="width: 100%; flex: 1; min-height: 400px;"></canvas>
                </div>
                
                <div id="reservoirFilters" style="width: 280px; min-width: 280px; border-left: 1px solid var(--border-color); padding-left: 1rem; overflow-y: auto; display: flex; flex-direction: column;">
                    <h3 style="margin: 0 0 1rem 0; font-size: 1rem; font-weight: 600; color: var(--primary-color);">
                        Reservoir Filters
                    </h3>
                    <div style="color: var(--text-secondary); text-align: center; padding: 2rem; flex: 1;">
                        Loading...
                    </div>
                </div>
            </div>
        `;
        
        modal.appendChild(modalContent);
        document.body.appendChild(modal);
        
        // Initialize the chart with a small delay to ensure Chart.js is loaded
        setTimeout(async () => {
            await this.initializeReservoirChart(specificReservoir);
        }, 100);
        
        // Add event listeners
        document.getElementById('timeRangeSelect').addEventListener('change', () => {
            this.updateReservoirChart(specificReservoir);
        });
        
        document.getElementById('chartViewSelect').addEventListener('change', () => {
            this.updateReservoirChart(specificReservoir);
        });
        
        document.getElementById('refreshChart').addEventListener('click', () => {
            this.updateReservoirChart(specificReservoir);
        });
        
        // Close modal on outside click
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                modal.remove();
            }
        });
        
        } catch (error) {
            console.error('Error opening reservoir trends:', error);
            this.showSafeMessage('Error loading reservoir trends: ' + error.message, 'error');
        }
    }
    
    async initializeReservoirChart(specificReservoir = null) {
        // Check if Chart.js is loaded
        if (typeof Chart === 'undefined') {
            console.error('Chart.js not loaded');
            document.getElementById('reservoirFilters').innerHTML = `
                <div style="color: var(--error-color); text-align: center; padding: 2rem;">
                    <p>Chart.js library not loaded</p>
                    <p>Please refresh the page</p>
                </div>
            `;
            return;
        }
        
        const canvas = document.getElementById('reservoirChart');
        if (!canvas) {
            console.error('Canvas element not found');
            return;
        }
        
        const ctx = canvas.getContext('2d');
        
        // Store chart instance for updates
        if (window.reservoirChart && typeof window.reservoirChart.destroy === 'function') {
            window.reservoirChart.destroy();
        }
        
            try {
                const days = document.getElementById('timeRangeSelect').value;
                const viewType = document.getElementById('chartViewSelect')?.value || 'daily';
                
                // Include reservoir mappings in the request
                const mappingsParam = encodeURIComponent(JSON.stringify(this.reservoirMapping));
                
                const url = specificReservoir 
                    ? `/api/v1/reservoir-trends?days=${days}&reservoirs=${encodeURIComponent(specificReservoir)}&view_type=${viewType}&mappings=${mappingsParam}`
                    : `/api/v1/reservoir-trends?days=${days}&view_type=${viewType}&mappings=${mappingsParam}`;
                
                console.log('Fetching reservoir trends from:', url);
            
            const response = await fetch(url);
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const data = await response.json();
            console.log('Received data:', data);
            
            if (!data.success) {
                throw new Error(data.error || 'Failed to fetch trend data');
            }
            
            // Create the chart
            window.reservoirChart = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: data.data.labels,
                    datasets: data.data.datasets
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    resizeDelay: 0,
                    interaction: {
                        intersect: false,
                        mode: 'index'
                    },
                    layout: {
                        padding: {
                            top: 10,
                            right: 10,
                            bottom: 10,
                            left: 10
                        }
                    },
                    plugins: {
                        title: {
                            display: true,
                            text: specificReservoir 
                                ? `${specificReservoir} ${viewType === 'cumulative' ? 'Cumulative' : 'Daily'} Activity`
                                : `All Reservoir ${viewType === 'cumulative' ? 'Cumulative' : 'Daily'} Activity`,
                            font: {
                                size: 16,
                                weight: 'bold'
                            },
                            color: '#1e293b'
                        },
                        legend: {
                            display: false  // We'll use custom filters
                        },
                        tooltip: {
                            backgroundColor: 'rgba(0,0,0,0.8)',
                            titleColor: 'white',
                            bodyColor: 'white',
                            borderColor: 'rgba(255,255,255,0.2)',
                            borderWidth: 1,
                            callbacks: {
                                title: function(tooltipItems) {
                                    const date = new Date(tooltipItems[0].label);
                                    return date.toLocaleDateString('en-US', { 
                                        weekday: 'long', 
                                        year: 'numeric', 
                                        month: 'long', 
                                        day: 'numeric' 
                                    });
                                },
                                label: function(context) {
                                    const labelText = viewType === 'cumulative' ? 'total permits' : 'permits';
                                    return `${context.dataset.label}: ${context.parsed.y} ${labelText}`;
                                }
                            }
                        }
                    },
                    scales: {
                        x: {
                            display: true,
                            title: {
                                display: true,
                                text: 'Date',
                                font: {
                                    weight: 'bold'
                                }
                            },
                            grid: {
                                color: 'rgba(0,0,0,0.1)'
                            }
                        },
                        y: {
                            display: true,
                            title: {
                                display: true,
                                text: viewType === 'cumulative' ? 'Cumulative Permits' : 'Number of Permits',
                                font: {
                                    weight: 'bold'
                                }
                            },
                            beginAtZero: true,
                            grid: {
                                color: 'rgba(0,0,0,0.1)'
                            }
                        }
                    }
                }
            });
            
            // Build the reservoir filter controls
            this.buildReservoirFilters(data.data.datasets, specificReservoir);
            
        } catch (error) {
            console.error('Error loading reservoir trends:', error);
            document.getElementById('reservoirFilters').innerHTML = `
                <div style="color: var(--error-color); text-align: center; padding: 2rem;">
                    <p>Failed to load trend data</p>
                    <button onclick="window.dashboard.updateReservoirChart('${specificReservoir || ''}')" 
                            style="padding: 0.5rem 1rem; background: var(--gradient-primary); color: white; border: none; border-radius: 0.375rem; cursor: pointer; margin-top: 1rem;">
                        Retry
                    </button>
                </div>
            `;
        }
    }
    
    buildReservoirFilters(datasets, specificReservoir) {
        const filtersContainer = document.getElementById('reservoirFilters');
        
        if (!datasets || datasets.length === 0) {
            filtersContainer.innerHTML = `
                <div style="color: var(--text-secondary); text-align: center; padding: 2rem;">
                    No reservoir data found
                </div>
            `;
            return;
        }
        
        let filtersHTML = `
            <div style="margin-bottom: 1rem;">
                <button onclick="window.dashboard.toggleAllReservoirs(true)" 
                        style="padding: 0.375rem 0.75rem; background: var(--gradient-primary); color: white; border: none; border-radius: 0.25rem; cursor: pointer; margin-right: 0.5rem; font-size: 0.75rem;">
                    Show All
                </button>
                <button onclick="window.dashboard.toggleAllReservoirs(false)" 
                        style="padding: 0.375rem 0.75rem; background: var(--text-secondary); color: white; border: none; border-radius: 0.25rem; cursor: pointer; font-size: 0.75rem;">
                    Hide All
                </button>
            </div>
        `;
        
        datasets.forEach((dataset, index) => {
            const isVisible = window.reservoirChart.isDatasetVisible(index);
            
            // Calculate total based on view type
            let total;
            const viewType = document.getElementById('chartViewSelect')?.value || 'daily';
            
            if (viewType === 'cumulative') {
                // For cumulative view, the total is the last (highest) value in the dataset
                total = Math.max(...dataset.data);
            } else {
                // For daily view, sum all the daily values
                total = dataset.data.reduce((sum, val) => sum + val, 0);
            }
            
            filtersHTML += `
                <div style="display: flex; align-items: center; gap: 0.5rem; padding: 0.5rem 0; border-bottom: 1px solid var(--border-color); cursor: pointer;"
                     onclick="window.dashboard.toggleReservoirVisibility(${index})">
                    <input type="checkbox" ${isVisible ? 'checked' : ''} 
                           style="margin: 0; accent-color: ${dataset.borderColor};">
                    <div style="width: 12px; height: 12px; background: ${dataset.borderColor}; border-radius: 2px; margin-right: 0.25rem;"></div>
                    <span style="font-size: 0.875rem; font-weight: 500; flex: 1;">${dataset.label}</span>
                    <span style="font-size: 0.75rem; color: var(--text-secondary);">
                        ${total} total
                    </span>
                </div>
            `;
        });
        
        filtersContainer.innerHTML = filtersHTML;
    }
    
    toggleReservoirVisibility(datasetIndex) {
        if (window.reservoirChart) {
            const isVisible = window.reservoirChart.isDatasetVisible(datasetIndex);
            window.reservoirChart.setDatasetVisibility(datasetIndex, !isVisible);
            window.reservoirChart.update();
            
            // Update the checkbox
            const checkbox = document.querySelectorAll('#reservoirFilters input[type="checkbox"]')[datasetIndex];
            if (checkbox) {
                checkbox.checked = !isVisible;
            }
        }
    }
    
    toggleAllReservoirs(show) {
        if (window.reservoirChart) {
            window.reservoirChart.data.datasets.forEach((dataset, index) => {
                window.reservoirChart.setDatasetVisibility(index, show);
            });
            window.reservoirChart.update();
            
            // Update all checkboxes
            document.querySelectorAll('#reservoirFilters input[type="checkbox"]').forEach(checkbox => {
                checkbox.checked = show;
            });
        }
    }
    
    async updateReservoirChart(specificReservoir = null) {
        await this.initializeReservoirChart(specificReservoir);
        
        // Force chart resize after update
        setTimeout(() => {
            if (window.reservoirChart) {
                window.reservoirChart.resize();
            }
        }, 100);
    }
    
    async updateParsingStats() {
        try {
            const response = await fetch('/api/v1/parsing/status');
            const data = await response.json();
            
            if (data.success) {
                this.renderParsingStats(data.stats);
            }
        } catch (error) {
            console.error('Error fetching parsing stats:', error);
            const parsingStatsEl = document.getElementById('parsingStats');
            if (parsingStatsEl) {
                parsingStatsEl.innerHTML = '<div style="color: var(--error-color); text-align: center;">Failed to load parsing stats</div>';
            }
        }
    }
    
    renderParsingStats(stats) {
        const parsingStatsEl = document.getElementById('parsingStats');
        if (!parsingStatsEl) return;
        
        const successRate = stats.success_rate || 0;
        const avgConfidence = stats.avg_confidence || 0;
        
        parsingStatsEl.innerHTML = `
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 0.75rem; margin-bottom: 0.75rem;">
                <div style="text-align: center;">
                    <div style="font-size: 1.5rem; font-weight: 600; color: var(--success-color);">${successRate.toFixed(1)}%</div>
                    <div style="font-size: 0.75rem; color: var(--text-secondary);">Success Rate</div>
                </div>
                <div style="text-align: center;">
                    <div style="font-size: 1.5rem; font-weight: 600; color: var(--primary-color);">${(avgConfidence * 100).toFixed(0)}%</div>
                    <div style="font-size: 0.75rem; color: var(--text-secondary);">Avg Confidence</div>
                </div>
            </div>
            
            <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.5rem 0; border-top: 1px solid var(--border-color); font-size: 0.875rem;">
                <span style="color: var(--text-secondary);">Pending:</span>
                <span style="font-weight: 500; color: var(--warning-color);">${stats.pending || 0}</span>
            </div>
            
            <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.5rem 0; border-top: 1px solid var(--border-color); font-size: 0.875rem;">
                <span style="color: var(--text-secondary);">Manual Review:</span>
                <span style="font-weight: 500; color: var(--error-color);">${stats.manual_review || 0}</span>
            </div>
        `;
    }
    
    async openParsingDashboard() {
        // Create the modal
        const modal = document.createElement('div');
        modal.className = 'parsing-modal';
        modal.style.cssText = `
            position: fixed; 
            top: 0; 
            left: 0; 
            right: 0; 
            bottom: 0; 
            background: rgba(0,0,0,0.7); 
            display: flex; 
            align-items: center; 
            justify-content: center; 
            z-index: 2000;
            animation: fadeIn 0.3s ease-out;
        `;
        
        const modalContent = document.createElement('div');
        modalContent.style.cssText = `
            background: white; 
            border-radius: 1rem; 
            width: 90vw; 
            height: 80vh; 
            max-width: 1000px; 
            position: relative;
            display: flex;
            flex-direction: column;
            box-shadow: 0 25px 50px -12px rgb(0 0 0 / 0.25);
        `;
        
        modalContent.innerHTML = `
            <div style="padding: 1.5rem; border-bottom: 1px solid var(--border-color); display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <h2 style="margin: 0; font-size: 1.5rem; font-weight: 600; color: var(--primary-color);">
                        🔍 Parsing Dashboard
                    </h2>
                    <p style="margin: 0.5rem 0 0 0; color: var(--text-secondary); font-size: 0.875rem;">
                        Monitor and manage permit parsing operations
                    </p>
                </div>
                <button onclick="this.closest('.parsing-modal').remove()" 
                        style="padding: 0.5rem; background: none; border: none; font-size: 1.5rem; cursor: pointer; color: var(--text-secondary);">
                    ✕
                </button>
            </div>
            
            <div style="padding: 1rem; display: flex; gap: 1rem; border-bottom: 1px solid var(--border-color); align-items: center;">
                <button id="processQueueBtn" style="padding: 0.5rem 1rem; background: var(--gradient-primary); color: white; border: none; border-radius: 0.375rem; cursor: pointer;">
                    🔄 Process Queue
                </button>
                <button id="refreshParsingBtn" style="padding: 0.5rem 1rem; background: var(--gradient-accent); color: white; border: none; border-radius: 0.375rem; cursor: pointer;">
                    📊 Refresh Stats
                </button>
            </div>
            
            <div style="flex: 1; padding: 1rem; display: flex; gap: 1rem;">
                <div style="flex: 1;">
                    <h3 style="margin: 0 0 1rem 0; font-size: 1rem; font-weight: 600; color: var(--primary-color);">
                        📈 Parsing Statistics
                    </h3>
                    <div id="detailedParsingStats">
                        <div style="color: var(--text-secondary); text-align: center; padding: 2rem;">
                            Loading...
                        </div>
                    </div>
                </div>
                
                <div style="width: 350px; border-left: 1px solid var(--border-color); padding-left: 1rem;">
                    <h3 style="margin: 0 0 1rem 0; font-size: 1rem; font-weight: 600; color: var(--error-color);">
                        ⚠️ Failed Jobs
                    </h3>
                    <div id="failedJobs" style="max-height: 400px; overflow-y: auto;">
                        <div style="color: var(--text-secondary); text-align: center; padding: 2rem;">
                            Loading...
                        </div>
                    </div>
                </div>
            </div>
        `;
        
        modal.appendChild(modalContent);
        document.body.appendChild(modal);
        
        // Load parsing data
        await this.loadParsingDashboardData();
        
        // Add event listeners
        document.getElementById('processQueueBtn').addEventListener('click', () => {
            this.processParsingQueue();
        });
        
        document.getElementById('refreshParsingBtn').addEventListener('click', async () => {
            const button = document.getElementById('refreshParsingBtn');
            const originalText = button.textContent;
            
            try {
                button.textContent = '⏳ Refreshing...';
                button.disabled = true;
                
                await this.loadParsingDashboardData();
                
                // Show success feedback
                button.textContent = '✅ Refreshed';
                setTimeout(() => {
                    button.textContent = originalText;
                }, 1500);
                
            } catch (error) {
                console.error('Error refreshing parsing dashboard:', error);
                button.textContent = '❌ Failed';
                setTimeout(() => {
                    button.textContent = originalText;
                }, 2000);
            } finally {
                button.disabled = false;
            }
        });
        
        // Close modal on outside click
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                modal.remove();
            }
        });
    }
    
    async loadParsingDashboardData() {
        try {
            // Load detailed stats
            const statsResponse = await fetch('/api/v1/parsing/status');
            const statsData = await statsResponse.json();
            
            if (statsData.success) {
                this.renderDetailedParsingStats(statsData.stats);
                
                // Show error message if there was an issue loading stats
                if (statsData.error) {
                    console.warn('Parsing stats loaded with error:', statsData.error);
                }
            } else {
                console.error('Failed to load parsing stats:', statsData);
                this.renderDetailedParsingStats({
                    total_jobs: 0,
                    pending: 0,
                    in_progress: 0,
                    success: 0,
                    failed: 0,
                    manual_review: 0,
                    success_rate: 0.0,
                    avg_confidence: 0.0
                });
            }
            
            // Load failed jobs
            const failedResponse = await fetch('/api/v1/parsing/failed');
            const failedData = await failedResponse.json();
            
            if (failedData.success) {
                this.renderFailedJobs(failedData.failed_jobs || []);
                
                // Show error message if there was an issue loading failed jobs
                if (failedData.error) {
                    console.warn('Failed jobs loaded with error:', failedData.error);
                }
            } else {
                console.error('Failed to load failed jobs:', failedData);
                this.renderFailedJobs([]);
            }
            
        } catch (error) {
            console.error('Error loading parsing dashboard data:', error);
            // Render empty state
            this.renderDetailedParsingStats({
                total_jobs: 0,
                pending: 0,
                in_progress: 0,
                success: 0,
                failed: 0,
                manual_review: 0,
                success_rate: 0.0,
                avg_confidence: 0.0
            });
            this.renderFailedJobs([]);
        }
    }
    
    renderDetailedParsingStats(stats) {
        const statsEl = document.getElementById('detailedParsingStats');
        if (!statsEl) return;
        
        statsEl.innerHTML = `
            <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 1rem; margin-bottom: 1.5rem;">
                <div style="background: var(--background-color); padding: 1rem; border-radius: 0.5rem; text-align: center;">
                    <div style="font-size: 2rem; font-weight: 600; color: var(--primary-color);">${stats.total_jobs || 0}</div>
                    <div style="font-size: 0.875rem; color: var(--text-secondary);">Total Jobs</div>
                </div>
                
                <div style="background: var(--background-color); padding: 1rem; border-radius: 0.5rem; text-align: center;">
                    <div style="font-size: 2rem; font-weight: 600; color: var(--success-color);">${stats.success || 0}</div>
                    <div style="font-size: 0.875rem; color: var(--text-secondary);">Successful</div>
                </div>
                
                <div style="background: var(--background-color); padding: 1rem; border-radius: 0.5rem; text-align: center;">
                    <div style="font-size: 2rem; font-weight: 600; color: var(--warning-color);">${stats.pending || 0}</div>
                    <div style="font-size: 0.875rem; color: var(--text-secondary);">Pending</div>
                </div>
                
                <div style="background: var(--background-color); padding: 1rem; border-radius: 0.5rem; text-align: center;">
                    <div style="font-size: 2rem; font-weight: 600; color: var(--error-color);">${stats.manual_review || 0}</div>
                    <div style="font-size: 0.875rem; color: var(--text-secondary);">Manual Review</div>
                </div>
            </div>
            
            <div style="background: var(--background-color); padding: 1rem; border-radius: 0.5rem;">
                <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                    <span style="font-weight: 500;">Success Rate:</span>
                    <span style="color: var(--success-color); font-weight: 600;">${(stats.success_rate || 0).toFixed(1)}%</span>
                </div>
                <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                    <span style="font-weight: 500;">Average Confidence:</span>
                    <span style="color: var(--primary-color); font-weight: 600;">${((stats.avg_confidence || 0) * 100).toFixed(1)}%</span>
                </div>
                <div style="display: flex; justify-content: space-between;">
                    <span style="font-weight: 500;">In Progress:</span>
                    <span style="color: var(--warning-color); font-weight: 600;">${stats.in_progress || 0}</span>
                </div>
            </div>
        `;
    }
    
    renderFailedJobs(jobs) {
        const failedEl = document.getElementById('failedJobs');
        if (!failedEl) return;
        
        if (jobs.length === 0) {
            failedEl.innerHTML = `
                <div style="color: var(--text-secondary); text-align: center; padding: 2rem;">
                    🎉 No failed jobs!
                </div>
            `;
            return;
        }
        
        failedEl.innerHTML = jobs.map(job => `
            <div style="background: var(--background-color); border-radius: 0.5rem; padding: 1rem; margin-bottom: 0.75rem; border-left: 4px solid var(--error-color);">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
                    <span style="font-weight: 600; color: var(--primary-color);">${job.status_no}</span>
                    <button onclick="window.dashboard.retryParsingJob('${job.permit_id}')" 
                            style="padding: 0.25rem 0.5rem; background: var(--gradient-primary); color: white; border: none; border-radius: 0.25rem; cursor: pointer; font-size: 0.75rem;">
                        🔄 Retry
                    </button>
                </div>
                <div style="font-size: 0.875rem; color: var(--text-secondary); margin-bottom: 0.25rem;">
                    Attempts: ${job.attempt_count} | Status: ${job.status}
                </div>
                ${job.error_message ? `
                    <div style="font-size: 0.75rem; color: var(--error-color); background: rgba(239, 68, 68, 0.1); padding: 0.5rem; border-radius: 0.25rem;">
                        ${job.error_message}
                    </div>
                ` : ''}
            </div>
        `).join('');
    }
    
    async processParsingQueue() {
        try {
            const button = document.getElementById('processQueueBtn');
            button.textContent = '⏳ Processing...';
            button.disabled = true;
            
            const response = await fetch('/api/v1/parsing/process', { method: 'POST' });
            const data = await response.json();
            
            if (data.success) {
                this.showSuccess(data.message || 'Parsing queue processed successfully');
                await this.loadParsingDashboardData();
            } else {
                this.showError(data.message || 'Failed to process parsing queue');
            }
            
        } catch (error) {
            console.error('Error processing parsing queue:', error);
            this.showError('Failed to process parsing queue');
        } finally {
            const button = document.getElementById('processQueueBtn');
            if (button) {
                button.textContent = '🔄 Process Queue';
                button.disabled = false;
            }
        }
    }
    
    async retryParsingJob(permitId) {
        try {
            const response = await fetch(`/api/v1/parsing/retry/${permitId}`, { method: 'POST' });
            const data = await response.json();
            
            if (data.success) {
                this.showSuccess(data.message || `Job ${permitId} queued for retry`);
                await this.loadParsingDashboardData();
            } else {
                this.showError(data.message || 'Failed to retry parsing job');
            }
            
        } catch (error) {
            console.error('Error retrying parsing job:', error);
            this.showError('Failed to retry parsing job');
        }
    }
    
    updateLastRefreshTime() {
        const timeStr = this.lastUpdate.toLocaleTimeString('en-US', {
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit'
        });
        document.getElementById('lastUpdate').textContent = `Last updated: ${timeStr}`;
    }
    
    clearFilters() {
        this.filters = { operator: '', county: '', purpose: '', queue: '' };
        document.getElementById('operatorFilter').value = '';
        document.getElementById('countyFilter').value = '';
        document.getElementById('purposeFilter').value = '';
        document.getElementById('queueFilter').value = '';
        this.applyFilters();
    }
    
    showLoading(show) {
        const loadingEl = document.getElementById('loading');
        const containerEl = document.getElementById('permitsContainer');
        
        if (show) {
            loadingEl.style.display = 'flex';
            if (containerEl) containerEl.style.opacity = '0.5';
        } else {
            loadingEl.style.display = 'none';
            if (containerEl) containerEl.style.opacity = '1';
        }
    }
    
    showError(message) {
        this.showToast(message, 'error');
    }
    
    showSuccess(message) {
        this.showToast(message, 'success');
    }
    
    showToast(message, type = 'info') {
        const toast = document.createElement('div');
        const bgColor = type === 'error' ? '#ef4444' : type === 'success' ? '#10b981' : '#3b82f6';
        toast.style.cssText = `
            position: fixed; 
            top: 1rem; 
            right: 1rem; 
            background: ${bgColor}; 
            color: white; 
            padding: 0.75rem 1rem; 
            border-radius: 0.5rem; 
            box-shadow: 0 10px 15px -3px rgb(0 0 0 / 0.1); 
            z-index: 1000;
            font-size: 0.875rem;
            font-weight: 500;
        `;
        toast.textContent = message;
        
        document.body.appendChild(toast);
        
        setTimeout(() => {
            toast.remove();
        }, 5000);
    }
    
    updateTopReservoirs() {
        // Count permits by reservoir (include dismissed permits for trend analysis)
        const reservoirCounts = {};
        this.permits.forEach(permit => {
            if (permit.field_name) {
                const reservoir = this.extractReservoir(permit.field_name);
                if (reservoir && reservoir !== 'UNKNOWN') {
                    reservoirCounts[reservoir] = (reservoirCounts[reservoir] || 0) + 1;
                }
            }
        });
        
        // Sort by count and get top 5
        const topReservoirs = Object.entries(reservoirCounts)
            .sort(([,a], [,b]) => b - a)
            .slice(0, 5);
        
        // Update the sidebar with reservoir stats
        const reservoirStatsEl = document.getElementById('reservoirStats');
        if (reservoirStatsEl) {
            if (topReservoirs.length === 0) {
                reservoirStatsEl.innerHTML = '<div style="color: var(--text-secondary); text-align: center; padding: 1rem;">No reservoirs found</div>';
                return;
            }
            
            reservoirStatsEl.innerHTML = topReservoirs.map(([reservoir, count]) => `
                <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.5rem 0; border-bottom: 1px solid var(--border-color); cursor: pointer;" 
                     onclick="window.dashboard.openReservoirTrends('${reservoir.replace(/'/g, "\\'")}')">
                    <span class="reservoir-display clickable-reservoir" style="font-size: 0.75rem;">${reservoir}</span>
                    <span style="font-size: 0.875rem; font-weight: 600; color: var(--primary-color);">${count}</span>
                </div>
            `).join('');
            
            // View All Trends button is now in the HTML template
        }
    }
    
    startAutoRefresh() {
        if (this.refreshTimer) {
            clearInterval(this.refreshTimer);
        }
        
        if (this.autoRefresh) {
            this.refreshTimer = setInterval(() => {
                this.loadPermits();
            }, this.refreshInterval);
        }
    }
    
    stopAutoRefresh() {
        if (this.refreshTimer) {
            clearInterval(this.refreshTimer);
            this.refreshTimer = null;
        }
    }
}

// Initialize dashboard when page loads
// Consolidated Dashboard with Mobile Support (Memory Optimized)
class OptimizedDashboard extends PermitDashboard {
    constructor() {
        super();
        
        // Mobile detection
        this.isMobile = window.innerWidth <= 768;
        
        // Memory management
        this.intervals = new Set();
        this.eventListeners = new WeakMap();
        this.isDestroyed = false;
        
        // Initialize features
        this.initMobileFeatures();
        this.initDataSync(); // Re-enabled after fixing main performance issue
        this.initCleanup();
    }

    // Memory-optimized initialization
    initMobileFeatures() {
        if (this.isMobile) {
            this.addTouchGestures();
            this.optimizeForMobile();
            this.handleOrientationChange();
        }
        
        // Throttled resize listener
        let resizeTimeout;
        const resizeHandler = () => {
            clearTimeout(resizeTimeout);
            resizeTimeout = setTimeout(() => {
                this.isMobile = window.innerWidth <= 768;
                if (this.isMobile) {
                    this.optimizeForMobile();
                }
            }, 250);
        };
        
        window.addEventListener('resize', resizeHandler);
        this.addCleanupTask(() => {
            window.removeEventListener('resize', resizeHandler);
            clearTimeout(resizeTimeout);
        });
    }
    
    // Cleanup management
    initCleanup() {
        // Initialize cleanup tasks array first
        if (!this.cleanupTasks) {
            this.cleanupTasks = [];
        }
        
        // Page unload cleanup
        const cleanup = () => this.destroy();
        window.addEventListener('beforeunload', cleanup);
        document.addEventListener('visibilitychange', () => {
            if (document.hidden) this.pauseBackgroundTasks();
            else this.resumeBackgroundTasks();
        });
        
        this.addCleanupTask(() => {
            window.removeEventListener('beforeunload', cleanup);
        });
    }
    
    addCleanupTask(task) {
        if (!this.cleanupTasks) {
            this.cleanupTasks = [];
        }
        if (typeof task === 'function') {
            this.cleanupTasks.push(task);
        }
    }
    
    // Safe interval management
    setManagedInterval(callback, delay) {
        const intervalId = setInterval(callback, delay);
        this.intervals.add(intervalId);
        return intervalId;
    }
    
    clearManagedInterval(intervalId) {
        clearInterval(intervalId);
        this.intervals.delete(intervalId);
    }

    // Mobile touch gestures (integrated and optimized)
    addTouchGestures() {
        if (!this.isMobile) return;
        
        // Pull-to-refresh functionality
        let startY = 0;
        let pullDistance = 0;
        const threshold = 100;

        const touchStartHandler = (e) => {
            if (window.scrollY === 0) {
                startY = e.touches[0].clientY;
            }
        };

        const touchMoveHandler = (e) => {
            if (window.scrollY === 0 && startY > 0) {
                const currentY = e.touches[0].clientY;
                pullDistance = currentY - startY;
                
                if (pullDistance > 0 && pullDistance < threshold * 2) {
                    document.body.style.transform = `translateY(${Math.min(pullDistance / 3, 50)}px)`;
                    document.body.style.transition = 'none';
                }
            }
        };

        const touchEndHandler = () => {
            if (pullDistance > threshold) {
                this.refreshData();
                this.showSafeMessage('🔄 Refreshing data...', 'info');
            }
            
            document.body.style.transform = '';
            document.body.style.transition = 'transform 0.3s ease';
            startY = 0;
            pullDistance = 0;
        };

        document.addEventListener('touchstart', touchStartHandler, { passive: true });
        document.addEventListener('touchmove', touchMoveHandler, { passive: true });
        document.addEventListener('touchend', touchEndHandler, { passive: true });

        // Add cleanup
        this.addCleanupTask(() => {
            document.removeEventListener('touchstart', touchStartHandler);
            document.removeEventListener('touchmove', touchMoveHandler);
            document.removeEventListener('touchend', touchEndHandler);
        });
    }

    // Mobile optimization methods (integrated)
    optimizeForMobile() {
        if (!this.isMobile) return;
        
        document.body.classList.add('mobile-optimized');
        
        // Optimize button spacing
        const buttons = document.querySelectorAll('.btn');
        buttons.forEach(btn => {
            if (!btn.style.minHeight) {
                btn.style.minHeight = '44px';
            }
        });

        this.addTouchFeedback();
        this.handleOrientationChange();
    }

    addTouchFeedback() {
        const interactiveElements = document.querySelectorAll('.btn, .permit-card, .reservoir-tab, .stat-card');
        
        interactiveElements.forEach(element => {
            const touchStartHandler = () => {
                element.style.transform = 'scale(0.98)';
                element.style.transition = 'transform 0.1s ease';
            };

            const touchEndHandler = () => {
                setTimeout(() => {
                    element.style.transform = '';
                    element.style.transition = 'transform 0.2s ease';
                }, 100);
            };

            element.addEventListener('touchstart', touchStartHandler, { passive: true });
            element.addEventListener('touchend', touchEndHandler, { passive: true });
            
            // Store for cleanup
            if (!this.eventListeners.has(element)) {
                this.eventListeners.set(element, []);
            }
            this.eventListeners.get(element).push(
                { type: 'touchstart', handler: touchStartHandler },
                { type: 'touchend', handler: touchEndHandler }
            );
        });
    }

    handleOrientationChange() {
        const orientationHandler = () => {
            setTimeout(() => {
                if (this.updateCharts && typeof this.updateCharts === 'function') {
                    this.updateCharts();
                }
                
                const vh = window.innerHeight * 0.01;
                document.documentElement.style.setProperty('--vh', `${vh}px`);
            }, 100);
        };

        window.addEventListener('orientationchange', orientationHandler);
        this.addCleanupTask(() => {
            window.removeEventListener('orientationchange', orientationHandler);
        });

        // Set initial viewport height
        const vh = window.innerHeight * 0.01;
        document.documentElement.style.setProperty('--vh', `${vh}px`);
    }

    // Dark mode functionality (integrated)
    initDarkMode() {
        this.darkMode = localStorage.getItem('darkMode') === 'true';
        this.applyDarkMode();
    }

    // Dark Mode Toggle (integrated into OptimizedDashboard)
    toggleDarkMode() {
        this.darkMode = !this.darkMode;
        localStorage.setItem('darkMode', this.darkMode.toString());
        this.applyDarkMode();
        
        // Haptic feedback
        this.addHapticFeedback('light');
    }

    applyDarkMode() {
        if (this.darkMode) {
            document.body.classList.add('dark-mode');
        } else {
            document.body.classList.remove('dark-mode');
        }
        
        // Update dark mode icon
        const icon = document.querySelector('.dark-mode-icon');
        if (icon) {
            icon.textContent = this.darkMode ? '☀️' : '🌙';
        }
    }
    
    // Add haptic feedback for supported devices
    addHapticFeedback(type = 'light') {
        if ('vibrate' in navigator) {
            const patterns = {
                light: [10],
                medium: [20],
                heavy: [30],
                success: [10, 50, 10],
                error: [50, 100, 50]
            };
            navigator.vibrate(patterns[type] || patterns.light);
        }
    }

    // Optimized data sync (reduced frequency)
    initDataSync() {
        // Sync data every 2 minutes when app is active (reduced from 30 seconds)
        this.syncInterval = this.setManagedInterval(() => {
            try {
                if (!document.hidden && !this.isDestroyed) {
                    this.syncData();
                }
            } catch (error) {
                console.error('Sync error:', error);
            }
        }, 120000); // 2 minutes instead of 30 seconds

        // Sync when app becomes visible (throttled)
        let visibilityTimeout;
        const visibilityHandler = () => {
            clearTimeout(visibilityTimeout);
            visibilityTimeout = setTimeout(() => {
                try {
                    if (!document.hidden && !this.isDestroyed) {
                        this.syncData();
                    }
                } catch (error) {
                    console.error('Visibility sync error:', error);
                }
            }, 1000); // 1 second throttle
        };
        
        document.addEventListener('visibilitychange', visibilityHandler);
        this.addCleanupTask(() => {
            document.removeEventListener('visibilitychange', visibilityHandler);
            clearTimeout(visibilityTimeout);
        });
    }
    
    // Pause/resume background tasks for memory optimization
    pauseBackgroundTasks() {
        this.backgroundPaused = true;
        // Clear intervals when page is hidden
        this.intervals.forEach(id => clearInterval(id));
        this.intervals.clear();
    }
    
    resumeBackgroundTasks() {
        if (this.backgroundPaused) {
            this.backgroundPaused = false;
            // Restart essential intervals only
            this.initDataSync();
        }
    }
    
    // Cleanup method
    destroy() {
        if (this.isDestroyed) return;
        
        this.isDestroyed = true;
        
        // Clear all intervals
        this.intervals.forEach(id => clearInterval(id));
        this.intervals.clear();
        
        // Run cleanup tasks
        this.cleanupTasks.forEach(task => {
            try {
                task();
            } catch (error) {
                console.error('Cleanup task error:', error);
            }
        });
        this.cleanupTasks = [];
        
        // Clear references
        this.eventListeners = null;
    }

    async syncData() {
        try {
            // Sync reservoir mappings
            const savedMappings = localStorage.getItem('reservoirMappings');
            const reviewQueue = localStorage.getItem('reservoirReviewQueue');
            
            if (savedMappings || reviewQueue) {
                // Send to server for sync across devices
                await fetch('/api/v1/sync/data', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        mappings: savedMappings ? JSON.parse(savedMappings) : {},
                        reviewQueue: reviewQueue ? JSON.parse(reviewQueue) : [],
                        timestamp: Date.now()
                    })
                });
            }
        } catch (error) {
            console.log('Sync not available:', error.message);
        }
    }

    // Add missing queue status class function
    getQueueStatusClass(queueStatus) {
        if (!queueStatus) return '';
        const q = queueStatus.toLowerCase();
        if (q.includes('mapping')) return 'status-mapping';
        if (q.includes('review')) return 'status-review';
        if (q.includes('queued')) return 'status-queued';
        if (q.includes('processing')) return 'status-processing';
        return '';
    }

    // Mobile button functions
    refreshData() {
        this.loadPermits();
        this.showMobileToast('🔄 Data refreshed', 'success');
    }

    showReservoirManagement() {
        // Create and show reservoir management modal
        const modal = this.createModal('Reservoir Management', this.generateReservoirManagementContent());
        document.body.appendChild(modal);
        modal.style.display = 'flex';
        
        // Set up tab switching functionality
        const tabs = modal.querySelectorAll('.reservoir-tab');
        const contents = modal.querySelectorAll('.reservoir-tab-content');
        
        tabs.forEach(tab => {
            tab.addEventListener('click', () => {
                // Remove active class from all tabs and contents
                tabs.forEach(t => t.classList.remove('active'));
                contents.forEach(c => c.classList.remove('active'));
                
                // Add active class to clicked tab
                tab.classList.add('active');
                
                // Show corresponding content
                const tabName = tab.getAttribute('data-tab');
                const content = modal.querySelector(`[data-content="${tabName}"]`);
                if (content) {
                    content.classList.add('active');
                    
                    // Load content based on tab
                    if (tabName === 'review') {
                        const reviewDiv = content.querySelector('#modal-review-queue');
                        if (reviewDiv) this.loadReviewQueueContent(reviewDiv);
                    } else if (tabName === 'saved') {
                        const savedDiv = content.querySelector('#modal-saved-mappings');
                        if (savedDiv) this.loadSavedMappingsContent(savedDiv);
                    }
                }
            });
        });
        
        // Load initial content (review tab)
        setTimeout(() => {
            const reviewDiv = modal.querySelector('#modal-review-queue');
            if (reviewDiv) this.loadReviewQueueContent(reviewDiv);
        }, 100);
    }

    generateReservoirManagementContent() {
        return `
            <div class="reservoir-management-modal">
                <div class="reservoir-tabs">
                    <button class="reservoir-tab active" data-tab="review">Under Review</button>
                    <button class="reservoir-tab" data-tab="saved">Saved Mappings</button>
                </div>
                <div class="reservoir-content">
                    <div class="reservoir-tab-content active" data-content="review">
                        <div id="modal-review-queue"></div>
                    </div>
                    <div class="reservoir-tab-content" data-content="saved">
                        <div id="modal-saved-mappings"></div>
                    </div>
                </div>
            </div>
        `;
    }

    exportPermits() {
        // Trigger export functionality
        if (typeof this.exportToExcel === 'function') {
            this.exportToExcel();
        } else {
            // Fallback: download as JSON
            const dataStr = JSON.stringify(this.permits, null, 2);
            const dataBlob = new Blob([dataStr], {type: 'application/json'});
            const url = URL.createObjectURL(dataBlob);
            const link = document.createElement('a');
            link.href = url;
            link.download = 'permits_export.json';
            link.click();
            URL.revokeObjectURL(url);
        }
        this.showMobileToast('📤 Export started', 'info');
    }

    // Modal functions for desktop and mobile
    showTopReservoirs() {
        console.log('📊 Opening Top Reservoirs modal...');
        
        // Create simple mobile modal for Top Reservoirs
        const modal = document.createElement('div');
        modal.className = 'mobile-modal-overlay';
        modal.style.cssText = 'position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.7); display: flex; align-items: center; justify-content: center; z-index: 1000; padding: 1rem;';
        
        // Generate reservoir data
        const reservoirCounts = {};
        this.permits.forEach(permit => {
            if (permit.field_name && permit.field_name !== 'Unknown') {
                const reservoir = this.extractReservoir(permit.field_name);
                if (reservoir && reservoir !== 'UNKNOWN') {
                    reservoirCounts[reservoir] = (reservoirCounts[reservoir] || 0) + 1;
                }
            }
        });

        const sortedReservoirs = Object.entries(reservoirCounts)
            .sort(([,a], [,b]) => b - a)
            .slice(0, 10);
        
        modal.innerHTML = `
            <div style="background: white; border-radius: 1rem; width: 100%; max-width: 500px; max-height: 90vh; overflow-y: auto; position: relative;">
                <div style="padding: 1.5rem; border-bottom: 1px solid #e5e7eb; position: sticky; top: 0; background: white; border-radius: 1rem 1rem 0 0;">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <h2 style="margin: 0; font-size: 1.25rem; font-weight: 600; color: #1f2937;">📊 Top Reservoirs</h2>
                        <button onclick="this.closest('.mobile-modal-overlay').remove()" style="background: none; border: none; font-size: 1.5rem; cursor: pointer; padding: 0.25rem;">✕</button>
                    </div>
                </div>
                <div style="padding: 1.5rem;">
                    ${sortedReservoirs.length === 0 ? 
                        '<div style="color: #6b7280; text-align: center; padding: 2rem;">No reservoir data available</div>' :
                        sortedReservoirs.map(([reservoir, count], index) => `
                            <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.75rem; border-bottom: 1px solid #e5e7eb; cursor: pointer;" onclick="console.log('Clicked reservoir: ${reservoir}')">
                                <div>
                                    <div style="font-weight: 600; color: #1f2937; font-size: 0.9rem;">${index + 1}. ${reservoir}</div>
                                </div>
                                <div style="font-weight: 700; color: #3b82f6; font-size: 1rem;">${count}</div>
                            </div>
                        `).join('')
                    }
                </div>
            </div>
        `;
        
        document.body.appendChild(modal);
        
        // Close on outside click
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                modal.remove();
            }
        });
    }

    showQuickStats() {
        console.log('📈 Opening Quick Stats modal...');
        
        // Create simple mobile modal for Quick Stats
        const modal = document.createElement('div');
        modal.className = 'mobile-modal-overlay';
        modal.style.cssText = 'position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.7); display: flex; align-items: center; justify-content: center; z-index: 1000; padding: 1rem;';
        
        // Calculate stats
        const totalPermits = this.permits.length;
        const todayPermits = this.permits.filter(permit => this.isPermitFromToday(permit)).length;
        const dismissedCount = this.dismissedPermits.size;
        const activePermits = totalPermits - dismissedCount;
        
        // Count by purpose
        const purposeCounts = {};
        this.permits.forEach(permit => {
            const purpose = permit.filing_purpose || 'Unknown';
            purposeCounts[purpose] = (purposeCounts[purpose] || 0) + 1;
        });
        
        modal.innerHTML = `
            <div style="background: white; border-radius: 1rem; width: 100%; max-width: 400px; max-height: 90vh; overflow-y: auto; position: relative;">
                <div style="padding: 1.5rem; border-bottom: 1px solid #e5e7eb; position: sticky; top: 0; background: white; border-radius: 1rem 1rem 0 0;">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <h2 style="margin: 0; font-size: 1.25rem; font-weight: 600; color: #1f2937;">📈 Quick Stats</h2>
                        <button onclick="this.closest('.mobile-modal-overlay').remove()" style="background: none; border: none; font-size: 1.5rem; cursor: pointer; padding: 0.25rem;">✕</button>
                    </div>
                </div>
                <div style="padding: 1.5rem;">
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin-bottom: 1.5rem;">
                        <div style="background: #f8fafc; padding: 1rem; border-radius: 0.5rem; text-align: center;">
                            <div style="font-size: 1.5rem; font-weight: 700; color: #3b82f6;">${totalPermits}</div>
                            <div style="font-size: 0.75rem; color: #6b7280; margin-top: 0.25rem;">Total Permits</div>
                        </div>
                        <div style="background: #f0fdf4; padding: 1rem; border-radius: 0.5rem; text-align: center;">
                            <div style="font-size: 1.5rem; font-weight: 700; color: #10b981;">${todayPermits}</div>
                            <div style="font-size: 0.75rem; color: #6b7280; margin-top: 0.25rem;">Today</div>
                        </div>
                        <div style="background: #fef3c7; padding: 1rem; border-radius: 0.5rem; text-align: center;">
                            <div style="font-size: 1.5rem; font-weight: 700; color: #f59e0b;">${activePermits}</div>
                            <div style="font-size: 0.75rem; color: #6b7280; margin-top: 0.25rem;">Active</div>
                        </div>
                        <div style="background: #fee2e2; padding: 1rem; border-radius: 0.5rem; text-align: center;">
                            <div style="font-size: 1.5rem; font-weight: 700; color: #ef4444;">${dismissedCount}</div>
                            <div style="font-size: 0.75rem; color: #6b7280; margin-top: 0.25rem;">Dismissed</div>
                        </div>
                    </div>
                    
                    <h3 style="margin: 0 0 1rem 0; font-size: 1rem; font-weight: 600;">By Purpose</h3>
                    ${Object.entries(purposeCounts)
                        .sort(([,a], [,b]) => b - a)
                        .slice(0, 5)
                        .map(([purpose, count]) => `
                            <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.5rem 0; border-bottom: 1px solid #e5e7eb;">
                                <span style="font-size: 0.8rem; color: #374151;">${purpose}</span>
                                <span style="font-weight: 600; color: #3b82f6;">${count}</span>
                            </div>
                        `).join('')
                    }
                </div>
            </div>
        `;
        
        document.body.appendChild(modal);
        
        // Close on outside click
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                modal.remove();
            }
        });
    }

    generateTopReservoirsContent() {
        const reservoirCounts = {};
        this.permits.forEach(permit => {
            if (permit.field_name && permit.field_name !== 'Unknown') {
                reservoirCounts[permit.field_name] = (reservoirCounts[permit.field_name] || 0) + 1;
            }
        });

        const sortedReservoirs = Object.entries(reservoirCounts)
            .sort(([,a], [,b]) => b - a)
            .slice(0, 10);

        return `
            <div style="max-height: 400px; overflow-y: auto;">
                ${sortedReservoirs.map(([reservoir, count], index) => `
                    <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.75rem; border-bottom: 1px solid var(--border-color);">
                        <div>
                            <div style="font-weight: 600; color: var(--text-primary);">${index + 1}. ${reservoir}</div>
                        </div>
                        <div style="font-weight: 700; color: var(--primary-color); font-size: 1.1rem;">${count}</div>
                    </div>
                `).join('')}
            </div>
        `;
    }

    generateQuickStatsContent() {
        const todayPermits = this.permits.filter(p => this.isPermitFromToday(p));
        const newDrillCount = this.permits.filter(p => p.purpose_code === 'NEW DRILL').length;
        const amendmentCount = this.permits.filter(p => p.purpose_code === 'AMENDMENT').length;

        return `
            <div style="display: grid; gap: 1rem;">
                <div style="text-align: center; padding: 1rem; background: var(--gradient-primary); color: white; border-radius: 0.5rem;">
                    <div style="font-size: 2rem; font-weight: 700;">${this.permits.length}</div>
                    <div style="font-size: 0.875rem; opacity: 0.9;">Total Permits</div>
                </div>
                <div style="text-align: center; padding: 1rem; background: var(--gradient-success); color: white; border-radius: 0.5rem;">
                    <div style="font-size: 2rem; font-weight: 700;">${todayPermits.length}</div>
                    <div style="font-size: 0.875rem; opacity: 0.9;">Today's Permits</div>
                </div>
                <div style="text-align: center; padding: 1rem; background: var(--gradient-accent); color: white; border-radius: 0.5rem;">
                    <div style="font-size: 2rem; font-weight: 700;">${newDrillCount}</div>
                    <div style="font-size: 0.875rem; opacity: 0.9;">New Drill Permits</div>
                </div>
                <div style="text-align: center; padding: 1rem; background: var(--gradient-warning); color: white; border-radius: 0.5rem;">
                    <div style="font-size: 2rem; font-weight: 700;">${amendmentCount}</div>
                    <div style="font-size: 0.875rem; opacity: 0.9;">Amendment Permits</div>
                </div>
                <div style="text-align: center; padding: 1rem; background: #8b5cf6; color: white; border-radius: 0.5rem;">
                    <div style="font-size: 2rem; font-weight: 700;">${this.reviewQueue.length}</div>
                    <div style="font-size: 0.875rem; opacity: 0.9;">Under Review</div>
                </div>
            </div>
        `;
    }

    createModal(title, content) {
        const modal = document.createElement('div');
        modal.className = 'modal';
        modal.innerHTML = `
            <div class="modal-content">
                <div class="modal-header">
                    <h2>${title}</h2>
                    <button class="modal-close" onclick="this.closest('.modal').remove()">×</button>
                </div>
                <div class="modal-body">
                    ${content}
                </div>
            </div>
        `;
        return modal;
    }

    // Enhanced permit rendering with "New Reservoir Detected" and "Injection Well" options
    renderPermitCard(permit) {
        // Get the original card HTML from the parent class method
        const originalCard = this.generatePermitCardHTML(permit);
        
        // Check if this permit is dismissed
        const isDismissed = this.dismissedPermits.has(permit.status_no);
        
        // Check if this is an injection well
        const isInjectionWell = this.isInjectionWell(permit);
        
        // Check if this is a new/unknown reservoir that needs attention (but not an injection well)
        const isNewReservoir = !isInjectionWell && permit.field_name && 
                              !this.reservoirMapping[permit.field_name] && 
                              permit.field_name !== 'Unknown' &&
                              !permit.field_name.includes('(exactly as shown in RRC records)') &&
                              !this.isIncorrectlyParsedFieldName(permit.field_name);

        if (isInjectionWell && !isDismissed) {
            // Add "Injection Well Detected" banner
            const cardElement = document.createElement('div');
            cardElement.innerHTML = originalCard;
            
            const banner = document.createElement('div');
            banner.className = 'injection-well-banner';
            
            banner.innerHTML = `
                <div style="background: linear-gradient(135deg, #dc2626, #b91c1c); color: white; padding: 0.5rem; border-radius: 0.375rem; margin-bottom: 0.75rem; text-align: center;">
                    <div style="font-weight: 600; font-size: 0.875rem;">🚫 Injection Well Detected</div>
                    <div style="font-size: 0.75rem; opacity: 0.9; margin-top: 0.25rem;">Purpose: ${permit.filing_purpose || 'Unknown'} • Click dismiss to hide</div>
                    <div style="margin-top: 0.5rem; display: flex; gap: 0.5rem; justify-content: center; flex-wrap: wrap;">
                        <button onclick="if(window.dashboard && window.dashboard.dismissPermit) window.dashboard.dismissPermit('${permit.status_no}')" 
                                style="background: #dc2626; border: none; color: white; padding: 0.375rem 0.75rem; border-radius: 0.25rem; font-size: 0.75rem; cursor: pointer;">
                            ✕ Dismiss Injection Well
                        </button>
                        <button onclick="if(window.dashboard && window.dashboard.openManualMappingForPermit) window.dashboard.openManualMappingForPermit(${JSON.stringify(permit).replace(/"/g, '&quot;')})" 
                                style="background: rgba(255,255,255,0.2); border: none; color: white; padding: 0.375rem 0.75rem; border-radius: 0.25rem; font-size: 0.75rem; cursor: pointer;">
                            ⚙️ Not Injection Well
                        </button>
                    </div>
                </div>
            `;
            
            const permitCard = cardElement.querySelector('.permit-card');
            if (permitCard) {
                permitCard.insertBefore(banner, permitCard.firstChild);
            }
            
            return cardElement.innerHTML;
        } else if (isNewReservoir) {
            // Add "New Reservoir Detected" banner
            const cardElement = document.createElement('div');
            cardElement.innerHTML = originalCard;
            
            const banner = document.createElement('div');
            banner.className = 'new-reservoir-banner';
            // Check if the field name looks like a valid reservoir (contains geological terms)
            const fieldName = permit.field_name || '';
            const looksLikeValidReservoir = this.isValidReservoirName(fieldName);
            
            banner.innerHTML = `
                <div style="background: linear-gradient(135deg, #f59e0b, #d97706); color: white; padding: 0.5rem; border-radius: 0.375rem; margin-bottom: 0.75rem; text-align: center;">
                    <div style="font-weight: 600; font-size: 0.875rem;">🆕 New Reservoir Detected</div>
                    <div style="font-size: 0.75rem; opacity: 0.9; margin-top: 0.25rem;">${fieldName}</div>
                    <div style="margin-top: 0.5rem; display: flex; gap: 0.5rem; justify-content: center; flex-wrap: wrap;">
                        ${looksLikeValidReservoir ? `
                            <button onclick="if(window.dashboard && window.dashboard.acceptNewReservoir) window.dashboard.acceptNewReservoir(${JSON.stringify(permit).replace(/"/g, '&quot;')})" 
                                    style="background: #10b981; border: none; color: white; padding: 0.375rem 0.75rem; border-radius: 0.25rem; font-size: 0.75rem; cursor: pointer;">
                                ✅ Accept as Correct
                            </button>
                        ` : ''}
                        <button onclick="if(window.dashboard && window.dashboard.openManualMappingForPermit) window.dashboard.openManualMappingForPermit(${JSON.stringify(permit).replace(/"/g, '&quot;')})" 
                                style="background: rgba(255,255,255,0.2); border: none; color: white; padding: 0.375rem 0.75rem; border-radius: 0.25rem; font-size: 0.75rem; cursor: pointer;">
                            ⚙️ Manage Reservoir
                        </button>
                    </div>
                </div>
            `;
            
            const permitCard = cardElement.querySelector('.permit-card');
            if (permitCard) {
                permitCard.insertBefore(banner, permitCard.firstChild);
                return cardElement.innerHTML;
            }
        }
        
        return originalCard;
    }

    toggleDarkMode() {
        const body = document.body;
        const isDark = body.classList.contains('dark-mode');
        
        if (isDark) {
            body.classList.remove('dark-mode');
            localStorage.setItem('darkMode', 'false');
            // Update button text
            const toggleBtn = document.getElementById('darkModeToggle');
            if (toggleBtn) {
                toggleBtn.innerHTML = '🌙 Night';
            }
        } else {
            body.classList.add('dark-mode');
            localStorage.setItem('darkMode', 'true');
            // Update button text
            const toggleBtn = document.getElementById('darkModeToggle');
            if (toggleBtn) {
                toggleBtn.innerHTML = '☀️ Day';
            }
        }
    }

    // Initialize dark mode from localStorage
    initializeDarkMode() {
        const savedMode = localStorage.getItem('darkMode');
        if (savedMode === 'true') {
            document.body.classList.add('dark-mode');
            const toggleBtn = document.getElementById('darkModeToggle');
            if (toggleBtn) {
                toggleBtn.innerHTML = '☀️ Day';
            }
        }
    }

    async showTrends() {
        console.log('📊 Opening Trends modal...');
        
        // Create mobile modal for trends with charts
        const modal = document.createElement('div');
        modal.className = 'mobile-modal-overlay trends-modal';
        modal.style.cssText = 'position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.7); display: flex; align-items: center; justify-content: center; z-index: 1000; padding: 0.5rem;';
        
        modal.innerHTML = `
            <div style="background: white; border-radius: 1rem; width: 100%; height: 95vh; position: relative; display: flex; flex-direction: column; overflow: hidden;">
                <div style="padding: 1rem; border-bottom: 1px solid #e5e7eb; position: sticky; top: 0; background: white; border-radius: 1rem 1rem 0 0; z-index: 10;">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <h2 style="margin: 0; font-size: 1.1rem; font-weight: 600; color: #1f2937;">📊 Reservoir Trends</h2>
                        <button onclick="this.closest('.trends-modal').remove()" style="background: none; border: none; font-size: 1.5rem; cursor: pointer; padding: 0.25rem;">✕</button>
                    </div>
                </div>
                
                <!-- Mobile Controls -->
                <div style="padding: 0.75rem; border-bottom: 1px solid #e5e7eb; background: #f8fafc;">
                    <div style="display: flex; gap: 0.5rem; align-items: center; flex-wrap: wrap;">
                        <select id="mobileTimeRange" style="padding: 0.5rem; border: 1px solid #d1d5db; border-radius: 0.375rem; font-size: 0.75rem; flex: 1; min-width: 100px;">
                            <option value="7">7 Days</option>
                            <option value="30">30 Days</option>
                            <option value="90" selected>90 Days</option>
                        </select>
                        <select id="mobileViewType" style="padding: 0.5rem; border: 1px solid #d1d5db; border-radius: 0.375rem; font-size: 0.75rem; flex: 1; min-width: 100px;">
                            <option value="daily">Daily</option>
                            <option value="cumulative">Cumulative</option>
                        </select>
                        <button id="mobileRefreshChart" style="padding: 0.5rem 0.75rem; background: #3b82f6; color: white; border: none; border-radius: 0.375rem; cursor: pointer; font-size: 0.7rem;">
                            🔄
                        </button>
                    </div>
                </div>
                
                <!-- Chart Container -->
                <div style="flex: 1; padding: 0.75rem; display: flex; flex-direction: column; min-height: 0; overflow: hidden;">
                    <div style="flex: 1; position: relative; min-height: 300px;">
                        <canvas id="mobileReservoirChart" style="width: 100%; height: 100%;"></canvas>
                    </div>
                    
                    <!-- Loading indicator -->
                    <div id="mobileChartLoading" style="position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); color: #6b7280; font-size: 0.875rem;">
                        Loading chart data...
                    </div>
                </div>
                
                <!-- Legend/Stats -->
                <div id="mobileTrendStats" style="padding: 0.75rem; border-top: 1px solid #e5e7eb; background: #f8fafc; max-height: 120px; overflow-y: auto;">
                    <div style="color: #6b7280; text-align: center; font-size: 0.75rem;">Chart will load here...</div>
                </div>
            </div>
        `;
        
        document.body.appendChild(modal);
        
        // Close on outside click
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                modal.remove();
            }
        });
        
        // Load initial chart
        await this.loadMobileTrendsChart();
        
        // Set up event listeners for controls
        document.getElementById('mobileTimeRange').addEventListener('change', () => this.loadMobileTrendsChart());
        document.getElementById('mobileViewType').addEventListener('change', () => this.loadMobileTrendsChart());
        document.getElementById('mobileRefreshChart').addEventListener('click', () => this.loadMobileTrendsChart());
    }

    async loadMobileTrendsChart() {
        const loadingEl = document.getElementById('mobileChartLoading');
        const statsEl = document.getElementById('mobileTrendStats');
        
        if (loadingEl) loadingEl.style.display = 'block';
        
        try {
            const timeRange = document.getElementById('mobileTimeRange')?.value || '90';
            const viewType = document.getElementById('mobileViewType')?.value || 'daily';
            
            // Get reservoir mappings from localStorage
            const reservoirMappings = JSON.parse(localStorage.getItem('reservoirMapping') || '{}');
            
            const response = await fetch(`/api/v1/reservoir-trends?days=${timeRange}&view_type=${viewType}&mappings=${encodeURIComponent(JSON.stringify(reservoirMappings))}`);
            const result = await response.json();
            
            if (!result.success) {
                throw new Error('Failed to load trend data');
            }
            
            const chartData = result.data;
            
            // Create or update chart
            const canvas = document.getElementById('mobileReservoirChart');
            const ctx = canvas.getContext('2d');
            
            // Destroy existing chart if it exists
            if (window.mobileTrendsChart) {
                window.mobileTrendsChart.destroy();
            }
            
            // Create new chart with mobile-optimized settings
            window.mobileTrendsChart = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: chartData.labels,
                    datasets: chartData.datasets.slice(0, 5) // Limit to top 5 for mobile
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            display: false // Hide legend to save space
                        },
                        tooltip: {
                            mode: 'index',
                            intersect: false,
                            titleFont: { size: 11 },
                            bodyFont: { size: 10 }
                        }
                    },
                    scales: {
                        x: {
                            display: true,
                            ticks: {
                                font: { size: 9 },
                                maxTicksLimit: 6 // Limit x-axis labels for mobile
                            }
                        },
                        y: {
                            display: true,
                            ticks: {
                                font: { size: 9 }
                            }
                        }
                    },
                    elements: {
                        point: {
                            radius: 2,
                            hoverRadius: 4
                        },
                        line: {
                            borderWidth: 2
                        }
                    }
                }
            });
            
            // Update stats section with legend
            if (statsEl) {
                const topReservoirs = chartData.datasets.slice(0, 5);
                statsEl.innerHTML = `
                    <div style="font-size: 0.7rem; font-weight: 600; margin-bottom: 0.5rem; color: #374151;">Top Reservoirs (${viewType}):</div>
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 0.25rem;">
                        ${topReservoirs.map(dataset => {
                            const total = dataset.data.reduce((sum, val) => sum + val, 0);
                            return `
                                <div style="display: flex; align-items: center; gap: 0.25rem;">
                                    <div style="width: 8px; height: 8px; border-radius: 50%; background: ${dataset.borderColor};"></div>
                                    <span style="font-size: 0.65rem; color: #374151; flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">${dataset.label}</span>
                                    <span style="font-size: 0.65rem; font-weight: 600; color: #3b82f6;">${total}</span>
                                </div>
                            `;
                        }).join('')}
                    </div>
                `;
            }
            
            if (loadingEl) loadingEl.style.display = 'none';
            
        } catch (error) {
            console.error('Error loading mobile trends chart:', error);
            if (loadingEl) {
                loadingEl.innerHTML = 'Error loading chart data';
                loadingEl.style.color = '#ef4444';
            }
        }
    }

    showMobileFilters() {
        // Create modal for mobile filters
        const modal = document.createElement('div');
        modal.className = 'mobile-modal-overlay';
        modal.style.cssText = 'position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.7); display: flex; align-items: flex-end; justify-content: center; z-index: 1000;';
        
        modal.innerHTML = `
            <div style="background: white; border-radius: 1rem 1rem 0 0; width: 100%; max-height: 80vh; overflow-y: auto; position: relative;">
                <div style="padding: 1.5rem; border-bottom: 1px solid #e5e7eb; position: sticky; top: 0; background: white; border-radius: 1rem 1rem 0 0;">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <h2 style="margin: 0; font-size: 1.25rem; font-weight: 600; color: #1f2937;">🔍 Filters & Controls</h2>
                        <button onclick="this.closest('.mobile-modal-overlay').remove()" style="background: none; border: none; font-size: 1.5rem; cursor: pointer; padding: 0.25rem;">✕</button>
                    </div>
                </div>
                <div style="padding: 1.5rem;">
                    <!-- Toggle Controls -->
                    <div style="margin-bottom: 1.5rem;">
                        <h3 style="margin: 0 0 1rem 0; font-size: 1rem; font-weight: 600;">View Options</h3>
                        <div style="display: flex; flex-direction: column; gap: 0.75rem;">
                            <label style="display: flex; align-items: center; gap: 0.5rem; cursor: pointer;">
                                <input type="checkbox" id="mobileShowDismissed" ${this.showDismissed ? 'checked' : ''} 
                                       onchange="window.dashboard.toggleShowDismissed()" 
                                       style="width: 18px; height: 18px;">
                                <span style="font-size: 0.875rem;">Show Dismissed Permits</span>
                            </label>
                            <label style="display: flex; align-items: center; gap: 0.5rem; cursor: pointer;">
                                <input type="checkbox" id="mobileTodayOnly" ${this.todayOnly ? 'checked' : ''} 
                                       onchange="window.dashboard.toggleTodayOnly()" 
                                       style="width: 18px; height: 18px;">
                                <span style="font-size: 0.875rem;">Today Only</span>
                            </label>
                        </div>
                    </div>
                    
                    <!-- Filter Controls -->
                    <div style="margin-bottom: 1.5rem;">
                        <h3 style="margin: 0 0 1rem 0; font-size: 1rem; font-weight: 600;">Search Filters</h3>
                        <div style="display: flex; flex-direction: column; gap: 1rem;">
                            <div>
                                <label style="display: block; margin-bottom: 0.5rem; font-size: 0.875rem; font-weight: 500;">Operator</label>
                                <input type="text" id="mobileOperatorFilter" placeholder="Search operator..." 
                                       value="${this.filters.operator || ''}"
                                       style="width: 100%; padding: 0.75rem; border: 1px solid #d1d5db; border-radius: 0.5rem; font-size: 0.875rem;">
                            </div>
                            <div>
                                <label style="display: block; margin-bottom: 0.5rem; font-size: 0.875rem; font-weight: 500;">County</label>
                                <input type="text" id="mobileCountyFilter" placeholder="Search county..." 
                                       value="${this.filters.county || ''}"
                                       style="width: 100%; padding: 0.75rem; border: 1px solid #d1d5db; border-radius: 0.5rem; font-size: 0.875rem;">
                            </div>
                            <div>
                                <label style="display: block; margin-bottom: 0.5rem; font-size: 0.875rem; font-weight: 500;">Purpose</label>
                                <select id="mobilePurposeFilter" 
                                        style="width: 100%; padding: 0.75rem; border: 1px solid #d1d5db; border-radius: 0.5rem; font-size: 0.875rem;">
                                    <option value="">All Purposes</option>
                                    <option value="new drill" ${this.filters.purpose === 'new drill' ? 'selected' : ''}>New Drill</option>
                                    <option value="amendment" ${this.filters.purpose === 'amendment' ? 'selected' : ''}>Amendment</option>
                                    <option value="reentry" ${this.filters.purpose === 'reentry' ? 'selected' : ''}>Reentry</option>
                                    <option value="recomplete" ${this.filters.purpose === 'recomplete' ? 'selected' : ''}>Recomplete</option>
                                </select>
                            </div>
                        </div>
                    </div>
                    
                    <!-- Action Buttons -->
                    <div style="display: flex; flex-direction: column; gap: 0.75rem;">
                        <button onclick="window.dashboard.applyMobileFilters()" 
                                style="width: 100%; padding: 0.75rem; background: linear-gradient(135deg, #3b82f6, #1d4ed8); color: white; border: none; border-radius: 0.5rem; font-size: 0.875rem; font-weight: 500;">
                            Apply Filters
                        </button>
                        <button onclick="window.dashboard.clearMobileFilters()" 
                                style="width: 100%; padding: 0.75rem; background: #6b7280; color: white; border: none; border-radius: 0.5rem; font-size: 0.875rem; font-weight: 500;">
                            Clear All Filters
                        </button>
                    </div>
                </div>
            </div>
        `;
        
        document.body.appendChild(modal);
        
        // Close on outside click
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                modal.remove();
            }
        });
    }

    applyMobileFilters() {
        // Get filter values from mobile modal
        const operatorFilter = document.getElementById('mobileOperatorFilter')?.value || '';
        const countyFilter = document.getElementById('mobileCountyFilter')?.value || '';
        const purposeFilter = document.getElementById('mobilePurposeFilter')?.value || '';
        
        // Update filters
        this.filters.operator = operatorFilter.toLowerCase();
        this.filters.county = countyFilter.toLowerCase();
        this.filters.purpose = purposeFilter.toLowerCase();
        
        // Apply filters
        this.applyFilters();
        
        // Close modal
        document.querySelector('.mobile-modal-overlay')?.remove();
        
        // Show success message
        const message = document.createElement('div');
        message.style.cssText = 'position: fixed; top: 20px; right: 20px; background: #10b981; color: white; padding: 12px 20px; border-radius: 8px; z-index: 1002; font-weight: 500;';
        message.textContent = '✅ Filters applied';
        document.body.appendChild(message);
        
        setTimeout(() => {
            if (message.parentNode) {
                message.remove();
            }
        }, 2000);
    }

    clearMobileFilters() {
        // Clear all filters
        this.filters = { operator: '', county: '', purpose: '', queue: '' };
        this.selectedOperators.clear();
        this.selectedCounties.clear();
        
        // Apply cleared filters
        this.applyFilters();
        
        // Close modal
        document.querySelector('.mobile-modal-overlay')?.remove();
        
        // Show success message
        const message = document.createElement('div');
        message.style.cssText = 'position: fixed; top: 20px; right: 20px; background: #10b981; color: white; padding: 12px 20px; border-radius: 8px; z-index: 1002; font-weight: 500;';
        message.textContent = '✅ Filters cleared';
        document.body.appendChild(message);
        
        setTimeout(() => {
            if (message.parentNode) {
                message.remove();
            }
        }, 2000);
    }

    // Helper method to generate permit card HTML (using original structure)
    generatePermitCardHTML(permit) {
        const isDismissed = this.dismissedPermits.has(permit.status_no);
        
        return `
            <div class="permit-card ${isDismissed ? 'dismissed' : ''}" data-permit-id="${permit.status_no}">
                <div class="permit-card-header">
                    <div class="permit-date">${this.formatDate(permit.created_at || permit.status_date)}</div>
                </div>
                
                <div class="permit-card-body">
                    <div class="permit-info-row" style="align-items: flex-start;">
                        <span class="permit-label" style="margin-top: 0;">Operator</span>
                        <span class="permit-value operator-name">${this.cleanOperatorName(permit.operator_name) || '-'}</span>
                    </div>
                    
                    <div class="permit-info-row">
                        <span class="permit-label">Lease</span>
                        <span class="permit-value lease-name">${permit.lease_name || '-'}</span>
                    </div>
                    
                    <div class="permit-info-row">
                        <span class="permit-label">Well #</span>
                        <span class="permit-value">${permit.well_no || '-'}</span>
                    </div>
                    
                    <div class="permit-info-row">
                        <span class="permit-label">Purpose</span>
                        <span class="permit-value">
                            <span class="permit-status ${this.getStatusClass(permit.filing_purpose)}">
                                ${permit.filing_purpose || '-'}
                            </span>
                        </span>
                    </div>
                    
                    <div class="permit-info-row">
                        <span class="permit-label">Queue</span>
                        <span class="permit-value">
                            <span class="permit-status ${this.getQueueStatusClass(permit.current_queue)}">
                                ${permit.current_queue || 'Not Queued'}
                            </span>
                        </span>
                    </div>
                    
                    <div class="permit-info-row">
                        <span class="permit-label">Drill Type</span>
                        <span class="permit-value">${permit.wellbore_profile || '-'}</span>
                    </div>
                    
                    <div class="permit-info-row">
                        <span class="permit-label">Reservoir</span>
                        <span class="permit-value reservoir-name" data-field="${permit.field_name}">
                            ${this.extractReservoir(permit.field_name)}
                        </span>
                    </div>
                </div>
                
                <div class="permit-card-actions">
                    ${permit.detail_url ? `
                        <a href="${permit.detail_url}" target="_blank" class="btn btn-sm btn-outline" style="text-decoration: none; display: inline-block;">
                            📄 View Permit
                        </a>
                    ` : ''}
                    
                    <button class="btn btn-sm btn-warning" onclick="window.dashboard.openManualMappingForPermit(${JSON.stringify(permit).replace(/"/g, '&quot;')})">
                        ⚙️ Manage Reservoir
                    </button>
                    
                    <button class="btn btn-sm btn-success" onclick="window.dashboard.flagForReenrich('${permit.status_no}')">
                        🔄 Re-enrich
                    </button>
                    
                    ${!isDismissed ? `
                        <button class="btn btn-sm btn-outline" onclick="window.dashboard.dismissPermit('${permit.status_no}')">
                            ✕ Dismiss
                        </button>
                    ` : `
                        <button class="btn btn-sm btn-outline" onclick="window.dashboard.undismissPermit('${permit.status_no}')">
                            ↩ Restore
                        </button>
                    `}
                </div>
            </div>
        `;
    }

    // Handle new reservoir detection
    handleNewReservoir(fieldName, statusNo) {
        // Add to review queue and open reservoir management
        this.addToReviewQueue(fieldName, 'Unknown - Needs Review');
        
        // Open reservoir management modal using the same function as mobile buttons
        this.showReservoirManagement();
        
        // Show success message
        this.showMobileToast(`🆕 "${fieldName}" added for review`, 'success');
    }

    // Safe message display (optimized for memory)
    showSafeMessage(message, type = 'info') {
        try {
            const toast = document.createElement('div');
            const bgColor = type === 'error' ? '#ef4444' : type === 'success' ? '#10b981' : '#3b82f6';
            
            // Use individual style properties for better compatibility
            toast.style.position = 'fixed';
            toast.style.top = '1rem';
            toast.style.right = '1rem';
            toast.style.background = bgColor;
            toast.style.color = 'white';
            toast.style.padding = '12px 20px';
            toast.style.borderRadius = '8px';
            toast.style.zIndex = '1000';
            toast.style.fontWeight = '500';
            toast.style.maxWidth = '300px';
            toast.style.wordWrap = 'break-word';
            
            toast.textContent = message;
            document.body.appendChild(toast);
            
            // Auto-remove with proper cleanup
            const removeToast = () => {
                try {
                    if (toast && toast.parentNode) {
                        toast.parentNode.removeChild(toast);
                    }
                } catch (e) {
                    // Ignore cleanup errors
                }
            };
            
            setTimeout(removeToast, 3000);
            
        } catch (error) {
            console.error('Message display error:', error);
            console.log(message); // Fallback
        }
    }

    // Refresh data method
    refreshData() {
        if (typeof this.loadPermitData === 'function') {
            this.loadPermitData();
        } else if (typeof this.loadPermits === 'function') {
            this.loadPermits();
        }
    }
    
    // Alias for compatibility
    showMobileToast(message, type = 'info') {
        this.showSafeMessage(message, type);
    }

    // Real-time store integration
    async refreshFromStore() {
        try {
            if (window.PermitStore) {
                const permits = await window.PermitStore.getAllPermits();
                // Update the permits array and re-render
                this.permits = permits || [];
                this.applyFilters(); // This will re-render the permit cards
                console.log(`🔄 Dashboard refreshed with ${this.permits.length} permits from store`);
            }
        } catch (error) {
            console.error('Error refreshing from store:', error);
        }
    }

    // Clear browser cache and force fresh data load
    async clearCacheAndRefresh() {
        try {
            console.log('🧹 Clearing browser cache and forcing fresh data load...');
            
            // Clear IndexedDB cache if available
            if (window.PermitStore && window.PermitStore.clearCache) {
                await window.PermitStore.clearCache();
            }
            
            // Clear localStorage cache
            const keysToRemove = [];
            for (let i = 0; i < localStorage.length; i++) {
                const key = localStorage.key(i);
                if (key && (key.startsWith('permit') || key.startsWith('reservoir'))) {
                    keysToRemove.push(key);
                }
            }
            keysToRemove.forEach(key => localStorage.removeItem(key));
            
            // Force fresh API load
            await this.loadPermits();
            
            console.log('✅ Cache cleared and data refreshed');
        } catch (error) {
            console.error('Error clearing cache:', error);
        }
    }

    // Count permits with the same field name (excluding current permit)
    async countPermitsWithSameFieldName(fieldName, excludeStatusNo) {
        try {
            const encodedFieldName = encodeURIComponent(fieldName);
            const url = `/api/v1/permits/count-by-field?field_name=${encodedFieldName}&exclude_status_no=${excludeStatusNo}`;
            console.log(`🔍 Count permits API call - Original: "${fieldName}", Encoded: "${encodedFieldName}", URL: ${url}`);
            
            const response = await fetch(url);
            if (!response.ok) {
                console.error(`Count permits API error: ${response.status} ${response.statusText}`);
                // Try to get error details
                try {
                    const errorData = await response.json();
                    console.error('API error details:', errorData);
                } catch (e) {
                    console.error('Could not parse error response');
                }
                return 0;
            }
            const data = await response.json();
            return data.count || 0;
        } catch (error) {
            console.error('Network error counting permits:', error);
            return 0;
        }
    }

    // Show confirmation dialog for bulk updates
    async showBulkUpdateConfirmation(count, wrongField, correctField) {
        return new Promise((resolve) => {
            const modal = document.createElement('div');
            modal.style.cssText = 'position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.7); display: flex; align-items: center; justify-content: center; z-index: 1003;';
            
            modal.innerHTML = `
                <div style="background: white; border-radius: 1rem; width: 90vw; max-width: 500px; padding: 2rem; box-shadow: 0 25px 50px -12px rgb(0 0 0 / 0.25);">
                    <div style="margin-bottom: 1.5rem; text-align: center;">
                        <div style="font-size: 3rem; margin-bottom: 1rem;">🔄</div>
                        <h2 style="margin: 0 0 0.5rem 0; font-size: 1.5rem; font-weight: 600; color: var(--primary-color);">
                            Bulk Update Available
                        </h2>
                        <p style="margin: 0; color: var(--text-secondary); font-size: 0.875rem;">
                            Found <strong>${count}</strong> other permit${count !== 1 ? 's' : ''} with the same incorrect field name.
                        </p>
                    </div>
                    
                    <div style="background: #f8fafc; border-radius: 0.5rem; padding: 1rem; margin-bottom: 1.5rem; font-size: 0.875rem;">
                        <div style="margin-bottom: 0.5rem;">
                            <strong>Wrong:</strong> <span style="color: #dc2626;">"${wrongField.length > 50 ? wrongField.substring(0, 50) + '...' : wrongField}"</span>
                        </div>
                        <div>
                            <strong>Correct:</strong> <span style="color: #059669;">"${correctField}"</span>
                        </div>
                    </div>
                    
                    <div style="display: flex; gap: 1rem; justify-content: flex-end;">
                        <button onclick="this.closest('.fixed').remove(); window.bulkUpdateResolve(false);" 
                                style="padding: 0.75rem 1.5rem; background: #6b7280; color: white; border: none; border-radius: 0.5rem; cursor: pointer;">
                            Skip Bulk Update
                        </button>
                        <button onclick="this.closest('.fixed').remove(); window.bulkUpdateResolve(true);" 
                                style="padding: 0.75rem 1.5rem; background: #059669; color: white; border: none; border-radius: 0.5rem; cursor: pointer; font-weight: 600;">
                            Update All ${count} Permits
                        </button>
                    </div>
                </div>
            `;
            
            document.body.appendChild(modal);
            
            // Set up global resolver
            window.bulkUpdateResolve = (result) => {
                delete window.bulkUpdateResolve;
                resolve(result);
            };
            
            // Close on outside click
            modal.addEventListener('click', (e) => {
                if (e.target === modal) {
                    modal.remove();
                    delete window.bulkUpdateResolve;
                    resolve(false);
                }
            });
        });
    }


    generateSavedMappingsContent() {
        const mappings = Object.entries(this.reservoirMapping);
        if (mappings.length === 0) {
            return '<div style="text-align: center; padding: 2rem; color: var(--text-secondary);">No saved mappings yet</div>';
        }
        
        return `
            <div style="display: grid; gap: 0.5rem;">
                ${mappings.map(([field, reservoir]) => `
                    <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.75rem; border: 1px solid var(--border-color); border-radius: 0.5rem;">
                        <div>
                            <div style="font-weight: 600;">${field}</div>
                            <div style="font-size: 0.875rem; color: var(--text-secondary);">→ ${reservoir}</div>
                        </div>
                        <button onclick="window.dashboard.removeSavedMapping('${field}')" style="background: var(--error-color); color: white; border: none; padding: 0.25rem 0.5rem; border-radius: 0.25rem; cursor: pointer;">
                            Remove
                        </button>
                    </div>
                `).join('')}
            </div>
        `;
    }

    generateReviewQueueContent() {
        if (this.reviewQueue.length === 0) {
            return '<div style="text-align: center; padding: 2rem; color: var(--text-secondary);">No items under review</div>';
        }
        
        return `
            <div style="display: grid; gap: 1rem;">
                ${this.reviewQueue.map(item => `
                    <div style="padding: 1rem; border: 1px solid var(--border-color); border-radius: 0.5rem;">
                        <div style="font-weight: 600; margin-bottom: 0.5rem;">${item.fieldName}</div>
                        <div style="font-size: 0.875rem; color: var(--text-secondary); margin-bottom: 1rem;">
                            Suggested: ${item.suggestedReservoir}
                        </div>
                        <div style="display: flex; gap: 0.5rem;">
                            <button onclick="window.dashboard.acceptReservoirSuggestion('${item.fieldName}', '${item.suggestedReservoir}')" 
                                    style="background: var(--success-color); color: white; border: none; padding: 0.5rem 1rem; border-radius: 0.25rem; cursor: pointer;">
                                Accept
                            </button>
                            <button onclick="window.dashboard.correctReservoirName('${item.fieldName}', ${JSON.stringify(item.permits)})" 
                                    style="background: var(--warning-color); color: white; border: none; padding: 0.5rem 1rem; border-radius: 0.25rem; cursor: pointer;">
                                Correct
                            </button>
                            <button onclick="window.dashboard.removeFromReviewQueue('${item.fieldName}')" 
                                    style="background: var(--error-color); color: white; border: none; padding: 0.5rem 1rem; border-radius: 0.25rem; cursor: pointer;">
                                Remove
                            </button>
                        </div>
                    </div>
                `).join('')}
            </div>
        `;
    }

    generateCancelledMappingsContent() {
        if (this.cancelledMappings.length === 0) {
            return '<div style="text-align: center; padding: 2rem; color: var(--text-secondary);">No cancelled mappings</div>';
        }
        
        return `
            <div style="display: grid; gap: 0.5rem;">
                ${this.cancelledMappings.map(mapping => `
                    <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.75rem; border: 1px solid var(--border-color); border-radius: 0.5rem; opacity: 0.7;">
                        <div>
                            <div style="font-weight: 600;">${mapping.fieldName}</div>
                            <div style="font-size: 0.875rem; color: var(--text-secondary);">Cancelled on ${new Date(mapping.cancelledAt).toLocaleDateString()}</div>
                        </div>
                        <button onclick="window.dashboard.restoreCancelledMapping('${mapping.fieldName}')" style="background: var(--primary-color); color: white; border: none; padding: 0.25rem 0.5rem; border-radius: 0.25rem; cursor: pointer;">
                            Restore
                        </button>
                    </div>
                `).join('')}
            </div>
        `;
    }

    // Duplicate function removed - using the main switchReservoirTab function above

    // Helper methods for reservoir management
    acceptReservoirSuggestion(fieldName, suggestedReservoir) {
        // Add to saved mappings
        this.reservoirMapping[fieldName] = suggestedReservoir;
        localStorage.setItem('reservoirMappings', JSON.stringify(this.reservoirMapping));
        
        // Remove from review queue
        this.removeFromReviewQueue(fieldName);
        
        // Show success message
        this.showMobileToast(`✅ Accepted: "${fieldName}" → "${suggestedReservoir}"`, 'success');
        
        // Refresh the modal content
        setTimeout(() => {
            const modal = document.querySelector('.modal');
            if (modal) {
                const modalBody = modal.querySelector('.modal-body');
                if (modalBody) {
                    modalBody.innerHTML = this.generateReservoirManagementContent();
                }
            }
        }, 1000);
    }

    removeSavedMapping(fieldName) {
        if (confirm(`Remove mapping for "${fieldName}"?`)) {
            delete this.reservoirMapping[fieldName];
            localStorage.setItem('reservoirMappings', JSON.stringify(this.reservoirMapping));
            
            // Refresh the modal content
            const modal = document.querySelector('.modal');
            if (modal) {
                const modalBody = modal.querySelector('.modal-body');
                if (modalBody) {
                    modalBody.innerHTML = this.generateReservoirManagementContent();
                }
            }
            
            this.showMobileToast(`🗑️ Removed mapping for "${fieldName}"`, 'info');
        }
    }

    restoreCancelledMapping(fieldName) {
        // Find the cancelled mapping
        const cancelledIndex = this.cancelledMappings.findIndex(m => m.fieldName === fieldName);
        if (cancelledIndex !== -1) {
            const cancelled = this.cancelledMappings[cancelledIndex];
            
            // Add back to review queue
            this.addToReviewQueue(cancelled.fieldName, 'Restored - Needs Review');
            
            // Remove from cancelled
            this.cancelledMappings.splice(cancelledIndex, 1);
            localStorage.setItem('cancelledMappings', JSON.stringify(this.cancelledMappings));
            
            // Refresh the modal content
            const modal = document.querySelector('.modal');
            if (modal) {
                const modalBody = modal.querySelector('.modal-body');
                if (modalBody) {
                    modalBody.innerHTML = this.generateReservoirManagementContent();
                }
            }
            
            this.showMobileToast(`🔄 Restored "${fieldName}" to review queue`, 'success');
        }
    }
}

document.addEventListener('DOMContentLoaded', () => {
    // Use single optimized dashboard instance
    window.dashboard = new OptimizedDashboard();
    
    // Cleanup on page unload
    window.addEventListener('beforeunload', () => {
        if (window.dashboard && typeof window.dashboard.destroy === 'function') {
            window.dashboard.destroy();
        }
    });
});

// Handle visibility change to pause/resume auto-refresh
document.addEventListener('visibilitychange', () => {
    if (window.dashboard) {
        if (document.hidden) {
            window.dashboard.stopAutoRefresh();
        } else if (window.dashboard.autoRefresh) {
            window.dashboard.startAutoRefresh();
        }
    }
});


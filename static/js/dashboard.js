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
        this.refreshInterval = 60000; // 1 minute
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
    }
    
    init() {
        this.loadDismissedPermits();
        this.loadReviewQueue();
        this.loadCancelledMappings();
        this.setupEventListeners();
        this.loadPermits();
        this.startAutoRefresh();
        this.updateStats();
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
            
            const response = await fetch('/api/v1/permits?limit=100');
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            const data = await response.json();
            this.permits = data.permits || [];
            this.lastUpdate = new Date();
            
            this.applyFilters();
            this.updateStats();
            this.updateLastRefreshTime();
            this.buildMultiSelectOptions();
            
        } catch (error) {
            console.error('Error loading permits:', error);
            this.showError('Failed to load permits. Please try again.');
        } finally {
            this.showLoading(false);
        }
    }
    
    applyFilters() {
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
            
            return (
                (!this.filters.operator || operator.includes(this.filters.operator)) &&
                (!this.filters.county || county.includes(this.filters.county)) &&
                (!this.filters.purpose || purpose.includes(this.filters.purpose)) &&
                (!this.filters.queue || queue.includes(this.filters.queue)) &&
                (this.showDismissed || !this.dismissedPermits.has(permit.status_no)) &&
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
        const today = new Date().toDateString();
        const permitDate = new Date(permit.status_date).toDateString();
        return permitDate === today;
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
                    <div class="permit-date">${this.formatDate(permit.status_date)}</div>
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
                        <a href="${permit.detail_url}" target="_blank" class="btn-open-permit">
                            📄 Open Permit
                        </a>
                    ` : `
                        <button class="btn-open-permit" disabled style="opacity: 0.5;">
                            📄 No URL Available
                        </button>
                    `}
                    <button class="btn-dismiss" data-permit-id="${permit.status_no}">
                        Dismiss
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
    
    dismissPermit(permitId) {
        this.dismissedPermits.add(permitId);
        
        // Add visual feedback
        const card = document.querySelector(`[data-permit-id="${permitId}"]`);
        if (card) {
            card.classList.add('dismissed');
            
            // Remove after animation
            setTimeout(() => {
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
                status_no: permit.status_no,
                lease_name: permit.lease_name,
                county: permit.county,
                detail_url: permit.detail_url
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
        return permit ? permit.id : null;
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
            // Add to saved mappings
            this.addToSavedMappings(currentFieldName, suggestedReservoir);
            
            // Remove this specific permit from review queue
            this.removeSinglePermitFromReview(currentFieldName, statusNo);
            
            // Show success message
            const successMsg = document.createElement('div');
            successMsg.style.cssText = 'position: fixed; top: 20px; right: 20px; background: #10b981; color: white; padding: 12px 20px; border-radius: 8px; z-index: 1000; font-weight: 500;';
            successMsg.textContent = `✅ Accepted "${suggestedReservoir}" as correct for permit ${statusNo}`;
            document.body.appendChild(successMsg);
            
            // Refresh tabs
            setTimeout(() => {
                document.body.removeChild(successMsg);
                this.updateReviewQueueDisplay();
                this.updateSavedMappingsDisplay();
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
            const response = await fetch('/api/v1/field-corrections/correct', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    permit_id: permit.id || this.getPermitIdByStatusNo(permit.status_no),
                    status_no: permit.status_no,
                    wrong_field: permit.currentFieldName,
                    correct_field: correctReservoir,
                    detail_url: permit.detail_url
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
                    this.loadPermitData();
                    this.updateReviewQueueDisplay();
                    this.updateSavedMappingsDisplay();
                }, 2000);
                
            } else {
                alert('Failed to correct permit');
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
        this.reviewQueue = this.reviewQueue.map(item => {
            if (item.fieldName === fieldName) {
                // Remove the specific permit from this item
                const updatedPermits = item.permits.filter(permit => permit.status_no !== statusNo);
                
                if (updatedPermits.length === 0) {
                    // If no permits left, mark for removal
                    return null;
                } else {
                    // Return item with updated permits list
                    return {
                        ...item,
                        permits: updatedPermits
                    };
                }
            }
            return item;
        }).filter(item => item !== null); // Remove null items
        
        // Save updated review queue
        localStorage.setItem('reviewQueue', JSON.stringify(this.reviewQueue));
        
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
        const reviewQueueEl = document.getElementById('reviewQueue');
        if (!reviewQueueEl) return;
        
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
        // Create the comprehensive reservoir management modal
        const modal = document.createElement('div');
        modal.className = 'fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50';
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
                
                <div style="padding: 1rem; border-top: 1px solid var(--border-color); text-align: right;">
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
        `;
        
        document.body.appendChild(modal);
        
        // Load the saved mappings tab by default
        this.switchReservoirTab('saved');
        
        // Close modal on outside click
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                modal.remove();
            }
        });
    }
    
    switchReservoirTab(tabName) {
        // Update tab buttons
        document.querySelectorAll('.reservoir-tab').forEach(tab => {
            tab.classList.remove('active');
        });
        document.getElementById(`${tabName}Tab`).classList.add('active');
        
        // Load content for the selected tab
        const contentDiv = document.getElementById('reservoirTabContent');
        
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

        contentDiv.innerHTML = `
            <div style="display: grid; gap: 1rem;">
                ${individualPermits.map(permit => `
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
            </div>
        `;
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

    async flagForReenrich(fieldName) {
        try {
            // Confirm with user
            const confirmed = confirm(
                `Flag permits with field name "${fieldName}" for re-enrichment?\n\n` +
                `This will re-extract detailed information for all permits with this field name. The process may take a few minutes.`
            );
            
            if (!confirmed) return;

            // Show loading state
            this.showInfo(`🔄 Flagging permits for re-enrichment...`);

            // Find all permits with this field name
            const permitsToReenrich = this.permits.filter(permit => 
                permit.field_name && permit.field_name.trim().toLowerCase() === fieldName.trim().toLowerCase()
            );

            if (permitsToReenrich.length === 0) {
                this.showError(`No permits found with field name "${fieldName}"`);
                return;
            }

            // Get existing re-enrichment queue
            const reenrichQueue = JSON.parse(localStorage.getItem('reenrichQueue') || '[]');
            
            // Add permits to re-enrichment queue
            let addedCount = 0;
            permitsToReenrich.forEach(permit => {
                // Check if already in queue
                const alreadyQueued = reenrichQueue.some(item => item.statusNo === permit.status_no);
                if (!alreadyQueued) {
                    reenrichQueue.push({
                        statusNo: permit.status_no,
                        leaseName: permit.lease_name,
                        fieldName: permit.field_name,
                        county: permit.county,
                        operator: permit.operator_name,
                        flaggedAt: new Date().toISOString(),
                        status: 'queued',
                        reason: 'Manual re-enrichment request'
                    });
                    addedCount++;
                }
            });

            // Save to localStorage
            localStorage.setItem('reenrichQueue', JSON.stringify(reenrichQueue));

            if (addedCount > 0) {
                this.showSuccess(`✅ Flagged ${addedCount} permit${addedCount !== 1 ? 's' : ''} for re-enrichment`);
                
                // Try to trigger server-side re-enrichment if API is available
                try {
                    const statusNumbers = reenrichQueue
                        .filter(item => item.status === 'queued')
                        .map(item => item.statusNo);
                    
                    if (statusNumbers.length > 0) {
                        const response = await fetch('/api/v1/permits/reenrich', {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json'
                            },
                            body: JSON.stringify({ 
                                status_numbers: statusNumbers.slice(0, 10), // Limit to 10 at a time
                                reason: 'Manual flag from dashboard'
                            })
                        });

                        if (response.ok) {
                            this.showSuccess(`🚀 Re-enrichment started for ${Math.min(statusNumbers.length, 10)} permits`);
                            
                            // Refresh permit data after a short delay
                            setTimeout(() => {
                                this.loadPermitData();
                            }, 3000);
                        } else {
                            console.warn('Re-enrichment API not available, permits queued locally');
                        }
                    }
                } catch (apiError) {
                    console.warn('Re-enrichment API not available:', apiError);
                    // Continue with local queuing
                }
                
            } else {
                this.showInfo(`All ${permitsToReenrich.length} permit${permitsToReenrich.length !== 1 ? 's' : ''} with this field name are already queued for re-enrichment`);
            }

            // Remove from review queue since we're handling it
            this.removeFromReviewQueue(fieldName);
            
        } catch (error) {
            console.error('Error flagging for re-enrichment:', error);
            this.showError(`Error: ${error.message}`);
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
            const date = new Date(dateStr);
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
        
        const todayPermits = activePermits.filter(p => {
            const today = new Date().toDateString();
            const permitDate = new Date(p.status_date || 0).toDateString();
            return permitDate === today;
        }).length;
        
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
            
            // Add "View All Trends" button
            reservoirStatsEl.innerHTML += `
                <div style="padding: 0.75rem 0; text-align: center;">
                    <button onclick="window.dashboard.openReservoirTrends()" 
                            style="padding: 0.5rem 1rem; background: var(--gradient-primary); color: white; border: none; border-radius: 0.375rem; cursor: pointer; font-size: 0.875rem;">
                        📈 View All Trends
                    </button>
                </div>
            `;
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
document.addEventListener('DOMContentLoaded', () => {
    window.dashboard = new PermitDashboard();
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


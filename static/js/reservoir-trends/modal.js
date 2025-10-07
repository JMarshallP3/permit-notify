/**
 * Enhanced Reservoir Trends Modal
 * Integrates state management, grouping, and chart functionality
 */

import { ReservoirTrendsState } from './state.js';
import { ReservoirTrendsChart } from './chart.js';

export class ReservoirTrendsModal {
    constructor() {
        this.modal = null;
        this.chart = null;
        this.currentState = null;
        this.isLoading = false;
    }

    /**
     * Open the enhanced reservoir trends modal
     */
    async open(specificReservoir = null) {
        // Load saved state
        this.currentState = await ReservoirTrendsState.loadState(
            ReservoirTrendsState.getCurrentContext().orgId,
            ReservoirTrendsState.getCurrentContext().userId
        );

        this.createModal(specificReservoir);
        this.setupEventListeners();
        
        // Load initial data
        await this.loadChartData();
    }

    /**
     * Create modal HTML structure
     */
    createModal(specificReservoir) {
        // Remove existing modal if present
        const existingModal = document.querySelector('.reservoir-trends-modal');
        if (existingModal) {
            existingModal.remove();
        }

        this.modal = document.createElement('div');
        this.modal.className = 'reservoir-trends-modal';
        this.modal.style.cssText = `
            position: fixed; 
            top: 0; 
            left: 0; 
            right: 0; 
            bottom: 0; 
            background: rgba(0,0,0,0.7); 
            display: flex; 
            align-items: center; 
            justify-content: center; 
            z-index: 1000; 
            padding: 1rem;
        `;

        const modalContent = document.createElement('div');
        modalContent.style.cssText = `
            background: white; 
            border-radius: 1rem; 
            width: 95vw; 
            height: 90vh; 
            max-width: 1400px; 
            position: relative;
            display: flex;
            flex-direction: column;
            box-shadow: 0 25px 50px -12px rgb(0 0 0 / 0.25);
        `;

        modalContent.innerHTML = `
            <!-- Header -->
            <div style="padding: 1.5rem; border-bottom: 1px solid #e5e7eb; display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <h2 style="margin: 0; font-size: 1.5rem; font-weight: 600; color: #1f2937;">
                        📈 Reservoir Trends Analysis
                    </h2>
                    <p style="margin: 0.5rem 0 0 0; color: #6b7280; font-size: 0.875rem;">
                        ${specificReservoir ? `Showing trends for ${specificReservoir}` : 'Historical permit activity by reservoir'}
                    </p>
                </div>
                <button id="closeModal" style="padding: 0.5rem; background: none; border: none; font-size: 1.5rem; cursor: pointer; color: #6b7280;">
                    ✕
                </button>
            </div>
            
            <!-- Enhanced Controls -->
            <div style="padding: 1rem; border-bottom: 1px solid #e5e7eb; background: #f8fafc;">
                <div style="display: flex; gap: 1rem; align-items: center; flex-wrap: wrap; margin-bottom: 1rem;">
                    <!-- Time Range -->
                    <div style="display: flex; gap: 0.5rem; align-items: center;">
                        <label style="font-weight: 500; color: #374151;">Time Range:</label>
                        <select id="timeRangeSelect" style="padding: 0.5rem; border: 1px solid #d1d5db; border-radius: 0.375rem;">
                            <option value="30">Last 30 Days</option>
                            <option value="90" selected>Last 90 Days</option>
                            <option value="180">Last 6 Months</option>
                            <option value="365">Last Year</option>
                            <option value="all">All Time</option>
                        </select>
                    </div>
                    
                    <!-- View Mode -->
                    <div style="display: flex; gap: 0.5rem; align-items: center;">
                        <label style="font-weight: 500; color: #374151;">View:</label>
                        <select id="chartViewSelect" style="padding: 0.5rem; border: 1px solid #d1d5db; border-radius: 0.375rem; min-width: 150px;">
                            <option value="daily">Daily Trends</option>
                            <option value="cumulative" selected>Cumulative Total</option>
                        </select>
                    </div>
                    
                    <!-- Refresh Button -->
                    <button id="refreshChart" style="padding: 0.5rem 1rem; background: #3b82f6; color: white; border: none; border-radius: 0.375rem; cursor: pointer;">
                        🔄 Refresh
                    </button>
                </div>
                
                <!-- Quick Controls -->
                <div style="display: flex; gap: 0.5rem; align-items: center; flex-wrap: wrap;">
                    <button id="hideAllBtn" style="padding: 0.375rem 0.75rem; background: #ef4444; color: white; border: none; border-radius: 0.375rem; cursor: pointer; font-size: 0.875rem;">
                        Hide All
                    </button>
                    <button id="showAllBtn" style="padding: 0.375rem 0.75rem; background: #10b981; color: white; border: none; border-radius: 0.375rem; cursor: pointer; font-size: 0.875rem;">
                        Show All
                    </button>
                    <button id="hideSingletonsBtn" style="padding: 0.375rem 0.75rem; background: #f59e0b; color: white; border: none; border-radius: 0.375rem; cursor: pointer; font-size: 0.875rem;">
                        Hide Singletons
                    </button>
                    <div style="display: flex; gap: 0.25rem; align-items: center;">
                        <button id="showTopNBtn" style="padding: 0.375rem 0.75rem; background: #8b5cf6; color: white; border: none; border-radius: 0.375rem; cursor: pointer; font-size: 0.875rem;">
                            Show Top
                        </button>
                        <select id="topNSelect" style="padding: 0.375rem; border: 1px solid #d1d5db; border-radius: 0.375rem; font-size: 0.875rem;">
                            <option value="5">5</option>
                            <option value="10" selected>10</option>
                            <option value="15">15</option>
                            <option value="20">20</option>
                        </select>
                    </div>
                    
                    <!-- Grouping Toggle -->
                    <div style="display: flex; gap: 0.5rem; align-items: center; margin-left: 1rem;">
                        <label style="display: flex; align-items: center; gap: 0.5rem; cursor: pointer; font-size: 0.875rem;">
                            <input type="checkbox" id="groupingToggle" style="margin: 0;">
                            <span>Enable Grouping</span>
                        </label>
                        <span id="groupingBadge" style="display: none; padding: 0.25rem 0.5rem; background: #dbeafe; color: #1e40af; border-radius: 0.25rem; font-size: 0.75rem; font-weight: 500;">
                            Grouping: ON (v${this.currentState.groupingVersion})
                        </span>
                    </div>
                </div>
            </div>
            
            <!-- Chart Container -->
            <div style="flex: 1; padding: 1rem; display: flex; gap: 1rem; min-height: 0;">
                <div style="flex: 1; min-width: 0; display: flex; flex-direction: column;">
                    <div style="position: relative; flex: 1; min-height: 400px;">
                        <canvas id="reservoirChart" style="width: 100%; height: 100%;"></canvas>
                        <div id="chartLoading" style="position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); color: #6b7280; font-size: 0.875rem;">
                            Loading chart data...
                        </div>
                    </div>
                </div>
                
                <!-- Legend/Sidebar -->
                <div id="reservoirFilters" style="width: 300px; min-width: 300px; border-left: 1px solid #e5e7eb; padding-left: 1rem; overflow-y: auto; display: flex; flex-direction: column;">
                    <h3 style="margin: 0 0 1rem 0; font-size: 1rem; font-weight: 600; color: #1f2937;">
                        Reservoir Controls
                    </h3>
                    <div id="legendContainer" style="flex: 1;">
                        <!-- Chart.js legend will be rendered here -->
                    </div>
                </div>
            </div>
        `;

        this.modal.appendChild(modalContent);
        document.body.appendChild(this.modal);
    }

    /**
     * Setup event listeners
     */
    setupEventListeners() {
        // Close modal
        document.getElementById('closeModal').addEventListener('click', () => {
            this.close();
        });

        // Close on backdrop click
        this.modal.addEventListener('click', (e) => {
            if (e.target === this.modal) {
                this.close();
            }
        });

        // Time range change
        document.getElementById('timeRangeSelect').addEventListener('change', (e) => {
            this.updateTimeRange(e.target.value);
        });

        // View mode change
        document.getElementById('chartViewSelect').addEventListener('change', (e) => {
            this.updateViewMode(e.target.value);
        });

        // Refresh button
        document.getElementById('refreshChart').addEventListener('click', () => {
            this.loadChartData();
        });

        // Quick control buttons
        document.getElementById('hideAllBtn').addEventListener('click', () => {
            this.chart?.showAllSeries(false);
        });

        document.getElementById('showAllBtn').addEventListener('click', () => {
            this.chart?.showAllSeries(true);
        });

        document.getElementById('hideSingletonsBtn').addEventListener('click', () => {
            this.chart?.hideSingletons();
        });

        document.getElementById('showTopNBtn').addEventListener('click', () => {
            const n = parseInt(document.getElementById('topNSelect').value);
            this.chart?.showTopN(n);
        });

        // Grouping toggle
        document.getElementById('groupingToggle').addEventListener('change', (e) => {
            this.toggleGrouping(e.target.checked);
        });

        // Apply saved state to UI
        this.applyStateToUI();
    }

    /**
     * Apply saved state to UI elements
     */
    applyStateToUI() {
        // Time range
        const timeRangeSelect = document.getElementById('timeRangeSelect');
        const timeRangeMap = {
            'last_30': '30',
            'last_90': '90',
            'last_180': '180',
            'last_365': '365',
            'all': 'all'
        };
        timeRangeSelect.value = timeRangeMap[this.currentState.timeRangeKey] || '90';

        // View mode
        document.getElementById('chartViewSelect').value = this.currentState.viewMode;

        // Grouping
        const groupingToggle = document.getElementById('groupingToggle');
        const groupingBadge = document.getElementById('groupingBadge');
        groupingToggle.checked = this.currentState.groupingEnabled;
        groupingBadge.style.display = this.currentState.groupingEnabled ? 'inline' : 'none';
    }

    /**
     * Load chart data from API
     */
    async loadChartData() {
        if (this.isLoading) return;
        
        this.isLoading = true;
        this.showLoading(true);

        try {
            const timeRange = document.getElementById('timeRangeSelect').value;
            const viewType = document.getElementById('chartViewSelect').value;
            
            const response = await fetch(`/api/v1/reservoir-trends?days=${timeRange}&view_type=${viewType}`);
            const result = await response.json();
            
            if (result.success) {
                await this.initializeChart(result.data);
            } else {
                throw new Error('Failed to load chart data');
            }
        } catch (error) {
            console.error('Error loading chart data:', error);
            this.showError('Failed to load chart data. Please try again.');
        } finally {
            this.isLoading = false;
            this.showLoading(false);
        }
    }

    /**
     * Initialize chart with data
     */
    async initializeChart(data) {
        // Destroy existing chart
        if (this.chart) {
            this.chart.destroy();
        }

        // Create new chart
        this.chart = new ReservoirTrendsChart('reservoirChart');
        
        // Set up state change handler
        this.chart.onStateChange = (state) => {
            this.saveState(state);
        };

        // Initialize with data
        await this.chart.initialize(data);
        
        // Apply saved state
        this.chart.applyState(this.currentState);
    }

    /**
     * Update time range
     */
    async updateTimeRange(days) {
        const timeRangeMap = {
            '30': 'last_30',
            '90': 'last_90',
            '180': 'last_180',
            '365': 'last_365',
            'all': 'all'
        };
        
        this.currentState.timeRangeKey = timeRangeMap[days] || 'last_90';
        await this.saveState();
        await this.loadChartData();
    }

    /**
     * Update view mode
     */
    async updateViewMode(mode) {
        this.currentState.viewMode = mode;
        await this.saveState();
        await this.loadChartData();
    }

    /**
     * Toggle grouping
     */
    async toggleGrouping(enabled) {
        this.currentState.groupingEnabled = enabled;
        
        const groupingBadge = document.getElementById('groupingBadge');
        groupingBadge.style.display = enabled ? 'inline' : 'none';
        
        if (this.chart) {
            this.chart.toggleGrouping(enabled);
        }
        
        await this.saveState();
    }

    /**
     * Save state
     */
    async saveState(additionalState = {}) {
        const context = ReservoirTrendsState.getCurrentContext();
        
        // Merge additional state (from chart)
        Object.assign(this.currentState, additionalState);
        
        await ReservoirTrendsState.saveState(
            context.orgId,
            context.userId,
            this.currentState
        );
    }

    /**
     * Show/hide loading indicator
     */
    showLoading(show) {
        const loadingEl = document.getElementById('chartLoading');
        if (loadingEl) {
            loadingEl.style.display = show ? 'block' : 'none';
        }
    }

    /**
     * Show error message
     */
    showError(message) {
        const loadingEl = document.getElementById('chartLoading');
        if (loadingEl) {
            loadingEl.textContent = message;
            loadingEl.style.color = '#ef4444';
            loadingEl.style.display = 'block';
        }
    }

    /**
     * Close modal
     */
    close() {
        if (this.chart) {
            this.chart.destroy();
        }
        
        if (this.modal) {
            this.modal.remove();
        }
    }
}

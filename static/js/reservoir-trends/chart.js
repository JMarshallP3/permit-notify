/**
 * Enhanced Chart.js implementation for Reservoir Trends
 * Handles hover behavior, performance optimizations, and state management
 */

import { ReservoirGrouping } from './grouping.js';

export class ReservoirTrendsChart {
    constructor(canvasId, options = {}) {
        this.canvasId = canvasId;
        this.canvas = document.getElementById(canvasId);
        this.chart = null;
        this.grouping = new ReservoirGrouping();
        this.debounceTimer = null;
        this.rawData = null;
        this.groupedData = null;
        this.currentViewData = null;
        this.hiddenSeries = new Set();
        this.colors = this.generateColors();
        
        // Configuration
        this.options = {
            debounceMs: 200,
            hoverThreshold: 10, // pixels
            ...options
        };
    }

    /**
     * Initialize the chart with data
     */
    async initialize(data) {
        this.rawData = data;
        this.groupedData = null;
        this.currentViewData = data;
        
        if (this.chart) {
            this.chart.destroy();
        }
        
        this.createChart();
        this.setupEventListeners();
    }

    /**
     * Create Chart.js instance with enhanced configuration
     */
    createChart() {
        const ctx = this.canvas.getContext('2d');
        
        this.chart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: this.currentViewData.labels,
                datasets: this.currentViewData.datasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    mode: 'nearest',
                    intersect: false,
                    axis: 'x'
                },
                plugins: {
                    legend: {
                        display: true,
                        position: 'right',
                        labels: {
                            usePointStyle: true,
                            padding: 15,
                            font: {
                                size: 11
                            }
                        },
                        onClick: (e, legendItem) => {
                            this.toggleSeriesVisibility(legendItem.datasetIndex);
                        }
                    },
                    tooltip: {
                        mode: 'nearest',
                        intersect: false,
                        backgroundColor: 'rgba(0, 0, 0, 0.8)',
                        titleColor: 'white',
                        bodyColor: 'white',
                        borderColor: 'rgba(255, 255, 255, 0.2)',
                        borderWidth: 1,
                        callbacks: {
                            title: (context) => {
                                return context[0].label;
                            },
                            label: (context) => {
                                const dataset = context.dataset;
                                const value = context.parsed.y;
                                const label = dataset.label;
                                
                                // Show only the hovered series
                                return `${label}: ${value}`;
                            },
                            afterBody: (context) => {
                                // Show original names if grouped
                                const dataset = context[0].dataset;
                                if (dataset.originalNames && dataset.originalNames.length > 1) {
                                    return [`Grouped from: ${dataset.originalNames.join(', ')}`];
                                }
                                return [];
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        type: 'time',
                        time: {
                            unit: 'day',
                            displayFormats: {
                                day: 'MMM DD'
                            }
                        },
                        title: {
                            display: true,
                            text: 'Date'
                        }
                    },
                    y: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'Permit Count'
                        }
                    }
                },
                elements: {
                    point: {
                        hoverRadius: 8
                    }
                },
                onHover: (event, elements) => {
                    // Enhanced hover behavior
                    this.handleHover(event, elements);
                }
            }
        });
    }

    /**
     * Handle hover events with enhanced behavior
     */
    handleHover(event, elements) {
        if (elements.length > 0) {
            const element = elements[0];
            const datasetIndex = element.datasetIndex;
            
            // Highlight only the hovered series
            this.chart.data.datasets.forEach((dataset, index) => {
                dataset.borderWidth = index === datasetIndex ? 3 : 1;
                dataset.pointRadius = index === datasetIndex ? 6 : 3;
            });
            
            this.chart.update('none');
        } else {
            // Reset all series to normal appearance
            this.chart.data.datasets.forEach(dataset => {
                dataset.borderWidth = 1;
                dataset.pointRadius = 3;
            });
            
            this.chart.update('none');
        }
    }

    /**
     * Toggle series visibility
     */
    toggleSeriesVisibility(datasetIndex) {
        const dataset = this.chart.data.datasets[datasetIndex];
        const isVisible = this.chart.isDatasetVisible(datasetIndex);
        
        this.chart.setDatasetVisibility(datasetIndex, !isVisible);
        this.chart.update();
        
        // Update hidden series set
        if (isVisible) {
            this.hiddenSeries.add(dataset.label);
        } else {
            this.hiddenSeries.delete(dataset.label);
        }
        
        // Trigger state save
        this.debouncedSaveState();
    }

    /**
     * Show/hide all series
     */
    showAllSeries(show = true) {
        this.chart.data.datasets.forEach((dataset, index) => {
            this.chart.setDatasetVisibility(index, show);
            
            if (show) {
                this.hiddenSeries.delete(dataset.label);
            } else {
                this.hiddenSeries.add(dataset.label);
            }
        });
        
        this.chart.update();
        this.debouncedSaveState();
    }

    /**
     * Hide singleton series (max count <= 1 in current range)
     */
    hideSingletons() {
        if (!this.currentViewData) return;
        
        const startDate = this.currentViewData.date_range.start;
        const endDate = this.currentViewData.date_range.end;
        
        // Convert to series format for analysis
        const series = this.grouping.datasetsToSeries(
            this.currentViewData.datasets,
            this.currentViewData.labels
        );
        
        const singletons = this.grouping.findSingletons(series, startDate, endDate);
        
        this.chart.data.datasets.forEach((dataset, index) => {
            if (singletons.includes(dataset.label)) {
                this.chart.setDatasetVisibility(index, false);
                this.hiddenSeries.add(dataset.label);
            }
        });
        
        this.chart.update();
        this.debouncedSaveState();
    }

    /**
     * Show only top N series
     */
    showTopN(n = 10) {
        if (!this.currentViewData) return;
        
        const startDate = this.currentViewData.date_range.start;
        const endDate = this.currentViewData.date_range.end;
        
        // Convert to series format for analysis
        const series = this.grouping.datasetsToSeries(
            this.currentViewData.datasets,
            this.currentViewData.labels
        );
        
        const topSeries = this.grouping.getTopNSeries(series, startDate, endDate, n);
        
        this.chart.data.datasets.forEach((dataset, index) => {
            const shouldShow = topSeries.includes(dataset.label);
            this.chart.setDatasetVisibility(index, shouldShow);
            
            if (shouldShow) {
                this.hiddenSeries.delete(dataset.label);
            } else {
                this.hiddenSeries.add(dataset.label);
            }
        });
        
        this.chart.update();
        this.debouncedSaveState();
    }

    /**
     * Toggle grouping on/off
     */
    toggleGrouping(enabled) {
        if (enabled && !this.groupedData) {
            // Generate grouped data
            const series = this.grouping.datasetsToSeries(
                this.rawData.datasets,
                this.rawData.labels
            );
            
            const groupedSeries = this.grouping.groupSeries(series);
            this.groupedData = {
                ...this.rawData,
                datasets: this.grouping.seriesToDatasets(
                    groupedSeries,
                    this.rawData.labels,
                    this.colors
                )
            };
        }
        
        this.currentViewData = enabled ? this.groupedData : this.rawData;
        this.rebuildChart();
        this.debouncedSaveState();
    }

    /**
     * Rebuild chart with current data
     */
    rebuildChart() {
        if (!this.chart) return;
        
        this.chart.data.labels = this.currentViewData.labels;
        this.chart.data.datasets = this.currentViewData.datasets;
        
        // Restore visibility state
        this.chart.data.datasets.forEach((dataset, index) => {
            const shouldShow = !this.hiddenSeries.has(dataset.label);
            this.chart.setDatasetVisibility(index, shouldShow);
        });
        
        this.chart.update();
    }

    /**
     * Update time range and view mode
     */
    async updateData(newData) {
        this.rawData = newData;
        this.groupedData = null;
        
        // Regenerate grouped data if grouping was enabled
        if (this.currentViewData === this.groupedData) {
            const series = this.grouping.datasetsToSeries(
                this.rawData.datasets,
                this.rawData.labels
            );
            
            const groupedSeries = this.grouping.groupSeries(series);
            this.groupedData = {
                ...this.rawData,
                datasets: this.grouping.seriesToDatasets(
                    groupedSeries,
                    this.rawData.labels,
                    this.colors
                )
            };
        }
        
        this.currentViewData = this.groupedData || this.rawData;
        this.rebuildChart();
    }

    /**
     * Apply saved state
     */
    applyState(state) {
        // Apply grouping
        if (state.groupingEnabled) {
            this.toggleGrouping(true);
        }
        
        // Apply hidden series
        this.hiddenSeries = new Set(state.hiddenSeries || []);
        this.rebuildChart();
    }

    /**
     * Get current state for saving
     */
    getCurrentState() {
        return {
            hiddenSeries: Array.from(this.hiddenSeries),
            groupingEnabled: this.currentViewData === this.groupedData
        };
    }

    /**
     * Debounced state save
     */
    debouncedSaveState() {
        if (this.debounceTimer) {
            clearTimeout(this.debounceTimer);
        }
        
        this.debounceTimer = setTimeout(() => {
            if (this.onStateChange) {
                this.onStateChange(this.getCurrentState());
            }
        }, this.options.debounceMs);
    }

    /**
     * Setup event listeners
     */
    setupEventListeners() {
        // Handle window resize
        window.addEventListener('resize', () => {
            if (this.chart) {
                this.chart.resize();
            }
        });
    }

    /**
     * Generate color palette
     */
    generateColors() {
        return [
            '#3B82F6', '#EF4444', '#10B981', '#F59E0B', '#8B5CF6',
            '#06B6D4', '#84CC16', '#F97316', '#EC4899', '#6366F1',
            '#14B8A6', '#F43F5E', '#8B5A2B', '#059669', '#DC2626',
            '#7C3AED', '#EA580C', '#BE185D', '#0891B2', '#65A30D'
        ];
    }

    /**
     * Destroy chart instance
     */
    destroy() {
        if (this.chart) {
            this.chart.destroy();
            this.chart = null;
        }
        
        if (this.debounceTimer) {
            clearTimeout(this.debounceTimer);
        }
    }

    /**
     * Get chart instance
     */
    getChart() {
        return this.chart;
    }
}

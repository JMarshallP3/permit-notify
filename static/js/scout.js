/**
 * Scout v2.1 Widget JavaScript
 * Replaces the "Coming Soon" modal with full Scout insights UI
 */

class ScoutWidget {
    constructor() {
        this.insights = [];
        this.filters = {
            county: '',
            operator: '',
            confidence: '',
            days: 30,
            breakouts_only: false,
            state_filter: 'default'
        };
        this.undoTimeouts = new Map(); // Track undo timeouts
    }

    async loadInsights() {
        try {
            const params = new URLSearchParams({
                org_id: 'default_org',
                ...this.filters
            });

            const response = await fetch(`/api/v1/scout/insights?${params}`);
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }

            const data = await response.json();
            this.insights = data.insights || [];
            this.renderInsights();
        } catch (error) {
            console.error('Error loading Scout insights:', error);
            this.showError('Failed to load insights');
        }
    }

    async testCrawl() {
        try {
            this.showInfo('Starting MRF crawl... This may take 30-60 seconds.');
            
            const response = await fetch('/api/v1/scout/crawl/mrf?org_id=default_org', {
                method: 'POST'
            });
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
            
            const data = await response.json();
            
            if (data.success) {
                this.showSuccess(`Crawl completed! ${data.message}`);
                // Reload insights to show new ones
                await this.loadInsights();
            } else {
                throw new Error(data.detail || 'Crawl failed');
            }
        } catch (error) {
            console.error('Error during test crawl:', error);
            this.showError(`Crawl failed: ${error.message}`);
        }
    }

    renderInsights() {
        const container = document.getElementById('scoutInsightsContainer');
        if (!container) return;

        if (this.insights.length === 0) {
            container.innerHTML = this.getEmptyState();
            return;
        }

        const insightsHtml = this.insights.map(insight => this.renderInsightCard(insight)).join('');
        container.innerHTML = insightsHtml;
    }

    renderInsightCard(insight) {
        const analyticsChips = this.renderAnalyticsChips(insight.analytics);
        const sourceLinks = insight.source_urls.map(source => 
            `<a href="${source.url}" target="_blank" rel="noopener">${source.label}</a>`
        ).join(' | ');

        const stateClass = insight.user_state === 'kept' ? 'insight-kept' : 
                          insight.user_state === 'dismissed' ? 'insight-dismissed' : '';

        return `
            <div class="insight-card ${stateClass}" data-insight-id="${insight.id}">
                ${insight.user_state === 'kept' ? '<div class="insight-badge kept-badge">Kept</div>' : ''}
                
                <div class="insight-header">
                    <h3 class="insight-title">${insight.title}</h3>
                    <div class="insight-actions">
                        ${this.renderInsightActions(insight)}
                    </div>
                </div>

                <div class="insight-content">
                    <div class="insight-section">
                        <strong>What happened:</strong>
                        <ul>
                            ${insight.what_happened.map(item => `<li>${item}</li>`).join('')}
                        </ul>
                    </div>

                    <div class="insight-section">
                        <strong>Why it matters:</strong>
                        <ul>
                            ${insight.why_it_matters.map(item => `<li>${item}</li>`).join('')}
                        </ul>
                    </div>

                    <div class="insight-meta">
                        <div class="confidence-section">
                            <strong>Confidence:</strong> 
                            <span class="confidence-${insight.confidence}">${insight.confidence.toUpperCase()}</span>
                            — ${insight.confidence_reasons.join(', ')}
                        </div>

                        ${analyticsChips ? `<div class="analytics-chips">${analyticsChips}</div>` : ''}

                        <div class="next-checks">
                            <strong>Next checks:</strong>
                            <ul>
                                ${insight.next_checks.map(check => `<li>${check}</li>`).join('')}
                            </ul>
                        </div>

                        <div class="sources">
                            <strong>Sources:</strong> ${sourceLinks}
                        </div>
                    </div>
                </div>

                ${insight.dismiss_reason ? `
                    <div class="dismiss-reason">
                        <strong>Dismiss reason:</strong> ${insight.dismiss_reason}
                    </div>
                ` : ''}
            </div>
        `;
    }

    renderInsightActions(insight) {
        if (insight.user_state === 'dismissed' || insight.user_state === 'archived') {
            return ''; // No actions for dismissed/archived items
        }

        const isKept = insight.user_state === 'kept';
        
        return `
            <button class="btn btn-sm ${isKept ? 'btn-outline' : 'btn-primary'}" 
                    onclick="scoutWidget.toggleKeep('${insight.id}', ${!isKept})">
                ${isKept ? '✓ Kept' : 'Keep'}
            </button>
            <button class="btn btn-sm btn-outline" 
                    onclick="scoutWidget.dismissInsight('${insight.id}')">
                Dismiss
            </button>
            <button class="btn btn-sm btn-ghost" 
                    onclick="scoutWidget.showDismissReasonModal('${insight.id}')">
                ⋯
            </button>
        `;
    }

    renderAnalyticsChips(analytics) {
        const chips = [];
        
        if (analytics.permit_velocity_7d) {
            chips.push(`<span class="analytics-chip">7d velocity ${analytics.permit_velocity_7d}</span>`);
        }
        if (analytics.is_breakout) {
            chips.push(`<span class="analytics-chip breakout">Breakout</span>`);
        }
        if (analytics.new_operator) {
            chips.push(`<span class="analytics-chip">New operator</span>`);
        }
        if (analytics.near_term_activity) {
            chips.push(`<span class="analytics-chip">Near-term 30-60d</span>`);
        }
        if (analytics.median_lag_permit_to_spud_days) {
            chips.push(`<span class="analytics-chip">~${analytics.median_lag_permit_to_spud_days}d to spud</span>`);
        }

        return chips.join('');
    }

    async toggleKeep(insightId, shouldKeep) {
        try {
            const response = await fetch(`/api/v1/scout/insights/${insightId}/state?org_id=default_org`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    state: shouldKeep ? 'kept' : 'default'
                })
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }

            const result = await response.json();
            
            // Show undo toast
            this.showUndoToast(
                shouldKeep ? 'Kept' : 'Unkept', 
                result.undo_token, 
                insightId
            );

            // Refresh insights
            await this.loadInsights();

        } catch (error) {
            console.error('Error toggling keep state:', error);
            this.showError('Failed to update insight');
        }
    }

    async dismissInsight(insightId, reason = null) {
        try {
            const response = await fetch(`/api/v1/scout/insights/${insightId}/state?org_id=default_org`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    state: 'dismissed',
                    dismiss_reason: reason
                })
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }

            const result = await response.json();
            
            // Show undo toast
            this.showUndoToast('Dismissed', result.undo_token, insightId);

            // Refresh insights
            await this.loadInsights();

        } catch (error) {
            console.error('Error dismissing insight:', error);
            this.showError('Failed to dismiss insight');
        }
    }

    showDismissReasonModal(insightId) {
        const modal = document.createElement('div');
        modal.className = 'modal-overlay';
        modal.style.cssText = 'position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.7); display: flex; align-items: center; justify-content: center; z-index: 1000;';
        
        modal.innerHTML = `
            <div style="background: white; border-radius: 0.5rem; padding: 1.5rem; max-width: 400px; width: 90%;">
                <h3 style="margin: 0 0 1rem 0;">Dismiss with reason</h3>
                <textarea id="dismissReasonText" placeholder="Optional: Why are you dismissing this insight?" 
                         style="width: 100%; height: 80px; padding: 0.5rem; border: 1px solid #ccc; border-radius: 0.25rem; resize: vertical;"></textarea>
                <div style="display: flex; gap: 0.5rem; margin-top: 1rem; justify-content: flex-end;">
                    <button onclick="this.closest('.modal-overlay').remove()" 
                            style="padding: 0.5rem 1rem; background: #6b7280; color: white; border: none; border-radius: 0.25rem; cursor: pointer;">
                        Cancel
                    </button>
                    <button onclick="scoutWidget.dismissWithReason('${insightId}'); this.closest('.modal-overlay').remove();" 
                            style="padding: 0.5rem 1rem; background: #ef4444; color: white; border: none; border-radius: 0.25rem; cursor: pointer;">
                        Dismiss
                    </button>
                </div>
            </div>
        `;
        
        document.body.appendChild(modal);
        
        // Focus textarea
        setTimeout(() => {
            modal.querySelector('#dismissReasonText').focus();
        }, 100);
    }

    dismissWithReason(insightId) {
        const reason = document.getElementById('dismissReasonText')?.value || null;
        this.dismissInsight(insightId, reason);
    }

    showUndoToast(action, undoToken, insightId) {
        // Clear any existing undo timeout for this insight
        if (this.undoTimeouts.has(insightId)) {
            clearTimeout(this.undoTimeouts.get(insightId));
        }

        const toast = document.createElement('div');
        toast.className = 'undo-toast';
        toast.style.cssText = 'position: fixed; top: 20px; right: 20px; background: #374151; color: white; padding: 12px 16px; border-radius: 8px; z-index: 1001; display: flex; align-items: center; gap: 12px;';
        
        toast.innerHTML = `
            <span>${action}. </span>
            <button onclick="scoutWidget.undoAction('${undoToken}', '${insightId}'); this.parentElement.remove();" 
                    style="background: #10b981; color: white; border: none; padding: 4px 8px; border-radius: 4px; cursor: pointer; font-size: 0.875rem;">
                Undo
            </button>
        `;
        
        document.body.appendChild(toast);
        
        // Auto-remove after 8 seconds
        const timeout = setTimeout(() => {
            if (toast.parentNode) {
                toast.remove();
            }
            this.undoTimeouts.delete(insightId);
        }, 8000);
        
        this.undoTimeouts.set(insightId, timeout);
    }

    async undoAction(undoToken, insightId) {
        try {
            const response = await fetch(`/api/v1/scout/insights/${insightId}/undo?org_id=default_org`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    undo_token: undoToken
                })
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }

            // Clear the timeout
            if (this.undoTimeouts.has(insightId)) {
                clearTimeout(this.undoTimeouts.get(insightId));
                this.undoTimeouts.delete(insightId);
            }

            // Refresh insights
            await this.loadInsights();

        } catch (error) {
            console.error('Error undoing action:', error);
            this.showError('Failed to undo action');
        }
    }

    renderFilters() {
        return `
            <div class="scout-filters">
                <div class="filter-row">
                    <select onchange="scoutWidget.updateFilter('state_filter', this.value)">
                        <option value="default" ${this.filters.state_filter === 'default' ? 'selected' : ''}>Default</option>
                        <option value="kept" ${this.filters.state_filter === 'kept' ? 'selected' : ''}>Kept</option>
                        <option value="dismissed" ${this.filters.state_filter === 'dismissed' ? 'selected' : ''}>Dismissed</option>
                        <option value="archived" ${this.filters.state_filter === 'archived' ? 'selected' : ''}>Archived</option>
                        <option value="all" ${this.filters.state_filter === 'all' ? 'selected' : ''}>All</option>
                    </select>
                    
                    <select onchange="scoutWidget.updateFilter('days', parseInt(this.value))">
                        <option value="7" ${this.filters.days === 7 ? 'selected' : ''}>7 days</option>
                        <option value="30" ${this.filters.days === 30 ? 'selected' : ''}>30 days</option>
                        <option value="90" ${this.filters.days === 90 ? 'selected' : ''}>90 days</option>
                    </select>
                    
                    <select onchange="scoutWidget.updateFilter('confidence', this.value)">
                        <option value="">All Confidence</option>
                        <option value="high" ${this.filters.confidence === 'high' ? 'selected' : ''}>High</option>
                        <option value="medium" ${this.filters.confidence === 'medium' ? 'selected' : ''}>Medium</option>
                        <option value="low" ${this.filters.confidence === 'low' ? 'selected' : ''}>Low</option>
                    </select>
                </div>
                
                <div class="filter-row">
                    <input type="text" placeholder="County..." value="${this.filters.county}" 
                           onchange="scoutWidget.updateFilter('county', this.value)">
                    <input type="text" placeholder="Operator..." value="${this.filters.operator}" 
                           onchange="scoutWidget.updateFilter('operator', this.value)">
                    
                    <label>
                        <input type="checkbox" ${this.filters.breakouts_only ? 'checked' : ''} 
                               onchange="scoutWidget.updateFilter('breakouts_only', this.checked)">
                        Breakouts only
                    </label>
                    
                    <button onclick="scoutWidget.testCrawl()" 
                            style="background-color: #28a745; color: white; border: none; padding: 8px 12px; border-radius: 4px; cursor: pointer; margin-left: 10px;">
                        🕷️ Test Crawl MRF
                    </button>
                </div>
            </div>
        `;
    }

    updateFilter(key, value) {
        this.filters[key] = value;
        this.loadInsights();
    }

    getEmptyState() {
        const messages = {
            'default': 'No insights yet — Scout will post here automatically.',
            'kept': 'No kept insights.',
            'dismissed': 'No dismissed insights.',
            'archived': 'No archived insights.',
            'all': 'No insights found.'
        };
        
        const message = messages[this.filters.state_filter] || messages['default'];
        
        return `
            <div class="empty-state">
                <div style="font-size: 2rem; margin-bottom: 1rem;">🔍</div>
                <p>${message}</p>
            </div>
        `;
    }

    showError(message) {
        const container = document.getElementById('scoutInsightsContainer');
        if (container) {
            container.innerHTML = `
                <div class="error-state">
                    <div style="color: #ef4444; font-size: 1.5rem; margin-bottom: 0.5rem;">⚠️</div>
                    <p>${message}</p>
                    <button onclick="scoutWidget.loadInsights()" class="btn btn-sm btn-primary">
                        Try Again
                    </button>
                </div>
            `;
        }
    }

    showInfo(message) {
        const container = document.getElementById('scoutInsightsContainer');
        if (container) {
            container.innerHTML = `
                <div class="info-state">
                    <div style="color: #3b82f6; font-size: 1.5rem; margin-bottom: 0.5rem;">ℹ️</div>
                    <p>${message}</p>
                </div>
            `;
        }
    }

    showSuccess(message) {
        const container = document.getElementById('scoutInsightsContainer');
        if (container) {
            container.innerHTML = `
                <div class="success-state">
                    <div style="color: #10b981; font-size: 1.5rem; margin-bottom: 0.5rem;">✅</div>
                    <p>${message}</p>
                </div>
            `;
        }
    }

    getFullUI() {
        return `
            <div class="scout-widget-content">
                ${this.renderFilters()}
                <div id="scoutInsightsContainer" class="insights-container">
                    <div class="loading-state">
                        <div>Loading insights...</div>
                    </div>
                </div>
            </div>
        `;
    }
}

// Global instance
window.scoutWidget = new ScoutWidget();

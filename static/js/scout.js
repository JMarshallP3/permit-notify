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
            
            // If we get empty insights and no specific message, try demo mode
            if (this.insights.length === 0 && !data.message) {
                console.log('🔄 No insights found, trying demo mode...');
                await this.loadDemoInsights();
                return;
            }
            
            this.renderInsights();
            
            // Show compatibility mode message if applicable
            if (data.message && data.message.includes('compatibility')) {
                this.showInfo(data.message);
            }
        } catch (error) {
            console.error('Error loading Scout insights:', error);
            
            // Fallback to demo insights
            try {
                console.log('🔄 Falling back to Scout v2.2 demo mode...');
                const demoResponse = await fetch('/api/v1/scout/insights/demo?org_id=default_org');
                const demoData = await demoResponse.json();
                
                if (demoData.success) {
                    this.insights = demoData.insights || [];
                    this.renderInsights();
                    this.showSuccess('🚀 Scout v2.2 Demo Mode Active - Enhanced analytics with multi-source intelligence!');
                } else {
                    throw new Error('Demo mode also failed');
                }
            } catch (demoError) {
                console.error('Demo mode failed:', demoError);
                this.showError('Failed to load insights');
            }
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
                this.showSuccess(`MRF crawl completed! ${data.message}`);
                // Reload insights to show new ones
                await this.loadInsights();
            } else {
                throw new Error(data.detail || 'MRF crawl failed');
            }
        } catch (error) {
            console.error('Error during MRF test crawl:', error);
            this.showError(`MRF crawl failed: ${error.message}`);
        }
    }
    
    async testCrawlAll() {
        try {
            this.showInfo('Starting all-sources crawl (news, PR, SEC, social, forums, gov bulletins)... This may take 60-120 seconds.');
            
            const response = await fetch('/api/v1/scout/crawl/all?org_id=default_org', {
                method: 'POST'
            });
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
            
            const data = await response.json();
            
            if (data.success) {
                this.showSuccess(`All-sources crawl completed! ${data.message}`);
                console.log('📊 Crawl results by source:', data.results.sources);
                // Reload insights to show new ones
                await this.loadInsights();
            } else {
                throw new Error(data.detail || 'All-sources crawl failed');
            }
        } catch (error) {
            console.error('Error during all-sources test crawl:', error);
            this.showError(`All-sources crawl failed: ${error.message}`);
        }
    }
    
    async loadDemoInsights() {
        try {
            this.showInfo('Loading Scout v2.2 demo insights with enhanced analytics...');
            
            const response = await fetch('/api/v1/scout/insights/demo?org_id=default_org');
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
            
            const data = await response.json();
            
            if (data.success) {
                this.insights = data.insights || [];
                console.log('📊 Demo insights loaded:', this.insights.length, 'insights');
                console.log('📊 First insight sample:', this.insights[0]);
                
                this.renderInsights();
                this.showSuccess('🚀 Scout v2.2 Demo Mode Active! Featuring multi-source intelligence, breakout detection, and deep analytics.');
                
                // Double-check the container after rendering
                const container = document.getElementById('scoutInsightsContainer');
                console.log('📊 Container after render:', container ? container.innerHTML.length + ' chars' : 'not found');
                console.log('📊 Container HTML preview:', container ? container.innerHTML.substring(0, 200) + '...' : 'not found');
                console.log('📊 Container element:', container);
            } else {
                throw new Error(data.detail || 'Demo insights failed');
            }
        } catch (error) {
            console.error('Error loading demo insights:', error);
            this.showError(`Demo insights failed: ${error.message}`);
        }
    }
    
    async setupDatabase() {
        try {
            this.showInfo('🔧 Setting up Scout v2.2 database tables... This may take 30-60 seconds.');
            
            const response = await fetch('/api/v1/scout/setup', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                }
            });
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
            
            const data = await response.json();
            
            if (data.success) {
                this.showSuccess('🎉 Database setup complete! Scout v2.2 is now ready for real insights. Try "All Sources" again!');
                console.log('✅ Database setup output:', data.output);
                
                // Automatically try to load real insights
                setTimeout(() => {
                    this.loadInsights();
                }, 2000);
            } else {
                throw new Error(data.detail || 'Database setup failed');
            }
        } catch (error) {
            console.error('Error setting up database:', error);
            this.showError(`Database setup failed: ${error.message}. You can continue using demo mode.`);
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
        try {
            const analyticsChips = this.renderAnalyticsChips(insight.analytics || {});
            const sourceLinks = (insight.source_urls || []).map(source => 
                `<a href="${source.url}" target="_blank" rel="noopener">${source.label}</a>`
            ).join(' | ');

            const stateClass = insight.user_state === 'kept' ? 'insight-kept' : 
                              insight.user_state === 'dismissed' ? 'insight-dismissed' : '';

            return `
                <div class="insight-card ${stateClass}" data-insight-id="${insight.id}">
                    ${insight.user_state === 'kept' ? '<div class="insight-badge kept-badge">Kept</div>' : ''}
                    
                    <div class="insight-header">
                        <h3 class="insight-title">${insight.title || 'Untitled Insight'}</h3>
                        <div class="insight-actions">
                            ${this.renderInsightActions(insight)}
                        </div>
                    </div>

                    <div class="insight-content">
                        <div class="insight-section">
                            <strong>What happened:</strong>
                            <ul>
                                ${(insight.what_happened || []).map(item => `<li>${item}</li>`).join('')}
                            </ul>
                        </div>

                        <div class="insight-section">
                            <strong>Why it matters:</strong>
                            <ul>
                                ${(insight.why_it_matters || []).map(item => `<li>${item}</li>`).join('')}
                            </ul>
                        </div>

                        <div class="insight-meta">
                            <div class="confidence-section">
                                <strong>Confidence:</strong> 
                                <span class="confidence-${insight.confidence || 'low'}">${(insight.confidence || 'low').toUpperCase()}</span>
                                — ${(insight.confidence_reasons || []).join(', ')}
                            </div>

                            ${analyticsChips ? `<div class="analytics-chips">${analyticsChips}</div>` : ''}

                            <div class="next-checks">
                                <strong>Next checks:</strong>
                                <ul>
                                    ${(insight.next_checks || []).map(check => `<li>${check}</li>`).join('')}
                                </ul>
                            </div>

                            <div class="sources">
                                <strong>Sources:</strong> ${sourceLinks || 'No sources available'}
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
        } catch (error) {
            console.error('Error rendering insight card:', error, insight);
            return `
                <div class="insight-card">
                    <div class="insight-header">
                        <h3 class="insight-title">Error Rendering Insight</h3>
                    </div>
                    <div class="insight-content">
                        <p>There was an error rendering this insight. Check console for details.</p>
                        <pre>${JSON.stringify(insight, null, 2)}</pre>
                    </div>
                </div>
            `;
        }
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
        
        // Velocity indicators
        if (analytics.permit_velocity_7d) {
            const velocity = analytics.permit_velocity_7d;
            const velocityClass = velocity > 0.5 ? 'high-velocity' : '';
            chips.push(`<span class="analytics-chip ${velocityClass}">7d velocity: ${velocity}/day</span>`);
        }
        
        if (analytics.permit_velocity_30d) {
            const velocity = analytics.permit_velocity_30d;
            const velocityClass = velocity > 0.3 ? 'high-velocity' : '';
            chips.push(`<span class="analytics-chip ${velocityClass}">30d velocity: ${velocity}/day</span>`);
        }
        
        // Breakout detection
        if (analytics.is_breakout) {
            const zscore = analytics.breakout_zscore ? ` (z=${analytics.breakout_zscore})` : '';
            chips.push(`<span class="analytics-chip breakout">🚀 Breakout${zscore}</span>`);
        }
        
        // Operator status
        if (analytics.is_new_operator) {
            chips.push(`<span class="analytics-chip new-operator">🆕 New operator</span>`);
        }
        
        // Activity prediction
        if (analytics.near_term_activity) {
            chips.push(`<span class="analytics-chip activity">📅 Near-term activity</span>`);
        }
        
        // Timing metrics
        if (analytics.median_days_permit_to_spud) {
            chips.push(`<span class="analytics-chip timing">⏱️ ${analytics.median_days_permit_to_spud}d to spud</span>`);
        }
        
        // Agreement score
        if (analytics.agreement_score) {
            const score = Math.round(analytics.agreement_score * 100);
            const scoreClass = score >= 80 ? 'high-agreement' : score >= 60 ? 'medium-agreement' : 'low-agreement';
            chips.push(`<span class="analytics-chip ${scoreClass}">🤝 ${score}% agreement</span>`);
        }

        return chips.join(' ');
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
                    
                    <select onchange="scoutWidget.updateFilter('source_type', this.value)">
                        <option value="">All Sources</option>
                        <option value="filing" ${this.filters.source_type === 'filing' ? 'selected' : ''}>SEC/Filings</option>
                        <option value="gov_bulletin" ${this.filters.source_type === 'gov_bulletin' ? 'selected' : ''}>Gov Bulletins</option>
                        <option value="pr" ${this.filters.source_type === 'pr' ? 'selected' : ''}>Press Releases</option>
                        <option value="news" ${this.filters.source_type === 'news' ? 'selected' : ''}>News</option>
                        <option value="forum" ${this.filters.source_type === 'forum' ? 'selected' : ''}>Forums</option>
                        <option value="social" ${this.filters.source_type === 'social' ? 'selected' : ''}>Social/X</option>
                        <option value="blog" ${this.filters.source_type === 'blog' ? 'selected' : ''}>Blogs</option>
                    </select>
                </div>
                
                <div class="filter-row">
                    <label>
                        <input type="checkbox" ${this.filters.breakouts_only ? 'checked' : ''} 
                               onchange="scoutWidget.updateFilter('breakouts_only', this.checked)">
                        Breakouts only
                    </label>
                    
                    <button onclick="scoutWidget.testCrawl()" 
                            style="background-color: #28a745; color: white; border: none; padding: 6px 10px; border-radius: 4px; cursor: pointer; margin-left: 10px; font-size: 12px;">
                        🕷️ MRF
                    </button>
                    
                    <button onclick="scoutWidget.testCrawlAll()" 
                            style="background-color: #007bff; color: white; border: none; padding: 6px 10px; border-radius: 4px; cursor: pointer; margin-left: 5px; font-size: 12px;">
                        🌐 All Sources
                    </button>
                    
                    <button onclick="scoutWidget.loadDemoInsights()" 
                            style="background-color: #6f42c1; color: white; border: none; padding: 6px 10px; border-radius: 4px; cursor: pointer; margin-left: 5px; font-size: 12px;">
                        🚀 Demo v2.2
                    </button>
                    
                    <button onclick="scoutWidget.setupDatabase()" 
                            style="background-color: #dc2626; color: white; border: none; padding: 6px 10px; border-radius: 4px; cursor: pointer; margin-left: 5px; font-size: 12px;">
                        🔧 Fix Database
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
        // Show success message as a temporary toast instead of overwriting insights
        console.log('✅ SUCCESS:', message);
        
        // Create a temporary toast notification
        const toast = document.createElement('div');
        toast.style.cssText = `
            position: fixed; top: 20px; right: 20px; z-index: 10000;
            background: #10b981; color: white; padding: 1rem; border-radius: 0.5rem;
            box-shadow: 0 4px 12px rgba(0,0,0,0.3); max-width: 400px;
            font-size: 0.875rem; line-height: 1.4;
        `;
        toast.innerHTML = `✅ ${message}`;
        document.body.appendChild(toast);
        
        // Remove toast after 5 seconds
        setTimeout(() => {
            if (toast.parentNode) {
                toast.parentNode.removeChild(toast);
            }
        }, 5000);
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

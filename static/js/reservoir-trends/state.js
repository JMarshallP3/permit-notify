/**
 * State management for Reservoir Trends modal
 * Handles persistence of user preferences and chart state
 */

export class ReservoirTrendsState {
    constructor() {
        this.viewMode = 'cumulative'; // 'cumulative' | 'daily'
        this.timeRangeKey = 'last_90'; // 'last_30' | 'last_90' | 'ytd' | 'all'
        this.hiddenSeries = []; // canonical names after grouping
        this.groupingEnabled = false;
        this.groupingVersion = 1; // bump if rules change
        this.showTopN = null; // null or number for "Show Top N" mode
    }

    /**
     * Load state from localStorage and optional API
     */
    static async loadState(orgId, userId) {
        const key = `reservoirTrends:${orgId}:${userId}`;
        
        try {
            const stored = localStorage.getItem(key);
            if (stored) {
                const parsed = JSON.parse(stored);
                const state = new ReservoirTrendsState();
                Object.assign(state, parsed);
                console.debug('📊 Loaded reservoir trends state:', state);
                return state;
            }
        } catch (error) {
            console.warn('Failed to load reservoir trends state:', error);
        }

        // Try to load from API as fallback
        try {
            const response = await fetch(`/api/user_prefs?key=${encodeURIComponent(key)}`);
            if (response.ok) {
                const apiState = await response.json();
                if (apiState) {
                    const state = new ReservoirTrendsState();
                    Object.assign(state, apiState);
                    console.debug('📊 Loaded reservoir trends state from API:', state);
                    return state;
                }
            }
        } catch (error) {
            console.debug('API state load failed (expected if endpoint not available):', error);
        }

        // Return default state
        return new ReservoirTrendsState();
    }

    /**
     * Save state to localStorage and optional API
     */
    static async saveState(orgId, userId, state) {
        const key = `reservoirTrends:${orgId}:${userId}`;
        
        try {
            localStorage.setItem(key, JSON.stringify(state));
            console.debug('📊 Saved reservoir trends state to localStorage:', state);
        } catch (error) {
            console.warn('Failed to save reservoir trends state to localStorage:', error);
        }

        // Try to save to API as well (non-blocking)
        try {
            await fetch('/api/user_prefs', {
                method: 'PATCH',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    key: key,
                    value: state
                })
            });
            console.debug('📊 Saved reservoir trends state to API');
        } catch (error) {
            console.debug('API state save failed (expected if endpoint not available):', error);
        }
    }

    /**
     * Get current user and org IDs from the page context
     */
    static getCurrentContext() {
        // Try to get from global dashboard state
        if (window.dashboard && window.dashboard.currentUser) {
            return {
                userId: window.dashboard.currentUser.id,
                orgId: window.dashboard.currentUser.orgs?.[0]?.id || 'default_org'
            };
        }

        // Fallback to default values
        return {
            userId: 'anonymous',
            orgId: 'default_org'
        };
    }

    /**
     * Update a specific property and save
     */
    static async updateState(property, value) {
        const context = this.getCurrentContext();
        const currentState = await this.loadState(context.orgId, context.userId);
        
        currentState[property] = value;
        await this.saveState(context.orgId, context.userId, currentState);
        
        return currentState;
    }

    /**
     * Reset state to defaults
     */
    static async resetState() {
        const context = this.getCurrentContext();
        const defaultState = new ReservoirTrendsState();
        await this.saveState(context.orgId, context.userId, defaultState);
        return defaultState;
    }
}

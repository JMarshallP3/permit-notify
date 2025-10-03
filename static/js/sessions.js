/**
 * Session Management JavaScript for PermitTracker
 * Handles viewing and managing user sessions
 */

class SessionManager {
    constructor() {
        this.apiBase = '';
        this.currentUser = null;
        this.sessions = [];
        this.init();
    }

    init() {
        this.setupEventListeners();
        this.loadUserData();
        this.loadSessions();
    }

    setupEventListeners() {
        // Logout button
        const logoutBtn = document.getElementById('logout-btn');
        if (logoutBtn) {
            logoutBtn.addEventListener('click', () => this.logout());
        }

        // Logout all button
        const logoutAllBtn = document.getElementById('logout-all-btn');
        if (logoutAllBtn) {
            logoutAllBtn.addEventListener('click', () => this.logoutAll());
        }
    }

    async loadUserData() {
        try {
            const response = await fetch(`${this.apiBase}/auth/me`, {
                credentials: 'include'
            });

            if (response.ok) {
                this.currentUser = await response.json();
                this.displayUserInfo();
            } else {
                this.redirectToLogin();
            }
        } catch (error) {
            console.error('Failed to load user data:', error);
            this.redirectToLogin();
        }
    }

    async loadSessions() {
        try {
            const response = await fetch(`${this.apiBase}/auth/sessions`, {
                credentials: 'include'
            });

            if (response.ok) {
                this.sessions = await response.json();
                this.displaySessions();
            } else {
                this.showError('Failed to load sessions');
            }
        } catch (error) {
            console.error('Failed to load sessions:', error);
            this.showError('Failed to load sessions');
        }
    }

    displayUserInfo() {
        const userDetails = document.getElementById('user-details');
        if (userDetails && this.currentUser) {
            userDetails.innerHTML = `
                <div class="user-info-item">
                    <label>Email:</label>
                    <span>${this.currentUser.email}</span>
                </div>
                <div class="user-info-item">
                    <label>Username:</label>
                    <span>${this.currentUser.username || 'Not set'}</span>
                </div>
                <div class="user-info-item">
                    <label>Account Created:</label>
                    <span>${new Date(this.currentUser.created_at).toLocaleDateString()}</span>
                </div>
                <div class="user-info-item">
                    <label>Organizations:</label>
                    <span>${this.currentUser.orgs.length} organization(s)</span>
                </div>
            `;
        }
    }

    displaySessions() {
        const sessionsList = document.getElementById('sessions-list');
        if (!sessionsList) return;

        if (this.sessions.length === 0) {
            sessionsList.innerHTML = '<p class="no-sessions">No active sessions found.</p>';
            return;
        }

        const sessionsHtml = this.sessions.map(session => {
            const isCurrent = session.is_current;
            const createdDate = new Date(session.created_at).toLocaleString();
            const expiresDate = new Date(session.expires_at).toLocaleString();
            
            return `
                <div class="session-item ${isCurrent ? 'current-session' : ''}">
                    <div class="session-info">
                        <div class="session-header">
                            <h4>${session.user_agent || 'Unknown Device'}</h4>
                            ${isCurrent ? '<span class="current-badge">Current Session</span>' : ''}
                        </div>
                        <div class="session-details">
                            <div class="session-detail">
                                <label>IP Address:</label>
                                <span>${session.ip_address || 'Unknown'}</span>
                            </div>
                            <div class="session-detail">
                                <label>Created:</label>
                                <span>${createdDate}</span>
                            </div>
                            <div class="session-detail">
                                <label>Expires:</label>
                                <span>${expiresDate}</span>
                            </div>
                        </div>
                    </div>
                    <div class="session-actions">
                        ${!isCurrent ? `<button class="btn-danger btn-small" onclick="sessionManager.revokeSession('${session.id}')">Revoke</button>` : ''}
                    </div>
                </div>
            `;
        }).join('');

        sessionsList.innerHTML = sessionsHtml;
    }

    async revokeSession(sessionId) {
        if (!confirm('Are you sure you want to revoke this session?')) {
            return;
        }

        try {
            const response = await fetch(`${this.apiBase}/auth/sessions/${sessionId}`, {
                method: 'DELETE',
                credentials: 'include'
            });

            if (response.ok) {
                this.showSuccess('Session revoked successfully');
                this.loadSessions(); // Reload sessions
            } else {
                this.showError('Failed to revoke session');
            }
        } catch (error) {
            console.error('Failed to revoke session:', error);
            this.showError('Failed to revoke session');
        }
    }

    async logoutAll() {
        if (!confirm('Are you sure you want to logout from all devices? This will end all your active sessions.')) {
            return;
        }

        try {
            const response = await fetch(`${this.apiBase}/auth/logout-all`, {
                method: 'POST',
                credentials: 'include'
            });

            if (response.ok) {
                const result = await response.json();
                this.showSuccess(`Logged out from ${result.revoked_sessions} devices`);
                setTimeout(() => {
                    window.location.href = '/login';
                }, 2000);
            } else {
                this.showError('Failed to logout from all devices');
            }
        } catch (error) {
            console.error('Failed to logout all:', error);
            this.showError('Failed to logout from all devices');
        }
    }

    async logout() {
        try {
            const response = await fetch(`${this.apiBase}/auth/logout`, {
                method: 'POST',
                credentials: 'include'
            });

            if (response.ok) {
                window.location.href = '/login';
            } else {
                this.showError('Failed to logout');
            }
        } catch (error) {
            console.error('Failed to logout:', error);
            this.showError('Failed to logout');
        }
    }

    showError(message) {
        // Create or update error message
        let errorDiv = document.getElementById('error-message');
        if (!errorDiv) {
            errorDiv = document.createElement('div');
            errorDiv.id = 'error-message';
            errorDiv.className = 'error-message';
            document.body.appendChild(errorDiv);
        }
        
        errorDiv.textContent = message;
        errorDiv.style.display = 'block';
        
        // Hide after 5 seconds
        setTimeout(() => {
            errorDiv.style.display = 'none';
        }, 5000);
    }

    showSuccess(message) {
        // Create or update success message
        let successDiv = document.getElementById('success-message');
        if (!successDiv) {
            successDiv = document.createElement('div');
            successDiv.id = 'success-message';
            successDiv.className = 'success-message';
            document.body.appendChild(successDiv);
        }
        
        successDiv.textContent = message;
        successDiv.style.display = 'block';
        
        // Hide after 3 seconds
        setTimeout(() => {
            successDiv.style.display = 'none';
        }, 3000);
    }

    redirectToLogin() {
        window.location.href = '/login';
    }
}

// Initialize session manager when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.sessionManager = new SessionManager();
});

// Export for use in other scripts
if (typeof module !== 'undefined' && module.exports) {
    module.exports = SessionManager;
}

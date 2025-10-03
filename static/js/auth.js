/**
 * Authentication JavaScript for PermitTracker
 * Handles login, registration, password reset, and session management
 */

class AuthManager {
    constructor() {
        this.apiBase = '';
        this.currentUser = null;
        this.refreshTimer = null;
        this.init();
    }

    init() {
        this.setupEventListeners();
        this.checkAuthStatus();
    }

    setupEventListeners() {
        // Login form
        const loginForm = document.getElementById('loginForm');
        if (loginForm) {
            loginForm.addEventListener('submit', (e) => this.handleLogin(e));
        }

        // Register form
        const registerForm = document.getElementById('registerForm');
        if (registerForm) {
            registerForm.addEventListener('submit', (e) => this.handleRegister(e));
        }

        // Forgot password form
        const forgotPasswordForm = document.getElementById('forgotPasswordForm');
        if (forgotPasswordForm) {
            forgotPasswordForm.addEventListener('submit', (e) => this.handleForgotPassword(e));
        }

        // Form switching
        const registerLink = document.getElementById('register-link');
        if (registerLink) {
            registerLink.addEventListener('click', (e) => this.showRegisterForm(e));
        }

        const loginLink = document.getElementById('login-link');
        if (loginLink) {
            loginLink.addEventListener('click', (e) => this.showLoginForm(e));
        }

        const forgotPasswordLink = document.getElementById('forgot-password-link');
        if (forgotPasswordLink) {
            forgotPasswordLink.addEventListener('click', (e) => this.showForgotPasswordForm(e));
        }

        const backToLoginLink = document.getElementById('back-to-login-link');
        if (backToLoginLink) {
            backToLoginLink.addEventListener('click', (e) => this.showLoginForm(e));
        }

        // Password confirmation validation
        const confirmPasswordInput = document.getElementById('reg-confirm-password');
        if (confirmPasswordInput) {
            confirmPasswordInput.addEventListener('input', () => this.validatePasswordConfirmation());
        }
    }

    async checkAuthStatus() {
        try {
            const response = await fetch(`${this.apiBase}/auth/me`, {
                credentials: 'include'
            });

            if (response.ok) {
                const userData = await response.json();
                this.currentUser = userData;
                this.startTokenRefresh();
                this.redirectToDashboard();
            } else {
                this.showLoginForm();
            }
        } catch (error) {
            console.warn('Auth check failed:', error);
            this.showLoginForm();
        }
    }

    async handleLogin(e) {
        e.preventDefault();
        this.showLoading(true);

        const formData = new FormData(e.target);
        const loginData = {
            email: formData.get('email'),
            password: formData.get('password')
        };

        try {
            const response = await fetch(`${this.apiBase}/auth/login`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                credentials: 'include',
                body: JSON.stringify(loginData)
            });

            const result = await response.json();

            if (response.ok) {
                this.currentUser = result.user;
                this.showSuccess('Login successful! Redirecting...');
                this.startTokenRefresh();
                setTimeout(() => this.redirectToDashboard(), 1000);
            } else {
                this.showError(result.detail || 'Login failed');
            }
        } catch (error) {
            this.showError('Network error. Please try again.');
        } finally {
            this.showLoading(false);
        }
    }

    async handleRegister(e) {
        e.preventDefault();
        this.showLoading(true);

        const formData = new FormData(e.target);
        const registerData = {
            email: formData.get('email'),
            password: formData.get('password'),
            username: formData.get('username') || null
        };

        // Validate password confirmation
        if (!this.validatePasswordConfirmation()) {
            this.showLoading(false);
            return;
        }

        try {
            const response = await fetch(`${this.apiBase}/auth/register`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                credentials: 'include',
                body: JSON.stringify(registerData)
            });

            const result = await response.json();

            if (response.ok) {
                this.currentUser = result.user;
                this.showSuccess('Registration successful! Redirecting...');
                this.startTokenRefresh();
                setTimeout(() => this.redirectToDashboard(), 1000);
            } else {
                this.showError(result.detail || 'Registration failed');
            }
        } catch (error) {
            this.showError('Network error. Please try again.');
        } finally {
            this.showLoading(false);
        }
    }

    async handleForgotPassword(e) {
        e.preventDefault();
        this.showLoading(true);

        const formData = new FormData(e.target);
        const resetData = {
            email: formData.get('email')
        };

        try {
            const response = await fetch(`${this.apiBase}/auth/request-password-reset`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(resetData)
            });

            const result = await response.json();

            if (response.ok) {
                this.showSuccess(result.message);
            } else {
                this.showError(result.detail || 'Failed to send reset email');
            }
        } catch (error) {
            this.showError('Network error. Please try again.');
        } finally {
            this.showLoading(false);
        }
    }

    validatePasswordConfirmation() {
        const password = document.getElementById('reg-password').value;
        const confirmPassword = document.getElementById('reg-confirm-password').value;
        
        if (password && confirmPassword && password !== confirmPassword) {
            this.showError('Passwords do not match');
            return false;
        }
        
        this.hideError();
        return true;
    }

    showLoginForm(e) {
        if (e) e.preventDefault();
        document.getElementById('login-form').style.display = 'block';
        document.getElementById('register-form').style.display = 'none';
        document.getElementById('forgot-password-form').style.display = 'none';
        this.hideMessages();
    }

    showRegisterForm(e) {
        if (e) e.preventDefault();
        document.getElementById('login-form').style.display = 'none';
        document.getElementById('register-form').style.display = 'block';
        document.getElementById('forgot-password-form').style.display = 'none';
        this.hideMessages();
    }

    showForgotPasswordForm(e) {
        if (e) e.preventDefault();
        document.getElementById('login-form').style.display = 'none';
        document.getElementById('register-form').style.display = 'none';
        document.getElementById('forgot-password-form').style.display = 'block';
        this.hideMessages();
    }

    showLoading(show) {
        const loading = document.getElementById('loading');
        if (loading) {
            loading.style.display = show ? 'block' : 'none';
        }
    }

    showError(message) {
        const errorElement = document.getElementById('error-message');
        if (errorElement) {
            errorElement.textContent = message;
            errorElement.style.display = 'block';
        }
        
        const successElement = document.getElementById('success-message');
        if (successElement) {
            successElement.style.display = 'none';
        }
    }

    showSuccess(message) {
        const successElement = document.getElementById('success-message');
        if (successElement) {
            successElement.textContent = message;
            successElement.style.display = 'block';
        }
        
        const errorElement = document.getElementById('error-message');
        if (errorElement) {
            errorElement.style.display = 'none';
        }
    }

    hideError() {
        const errorElement = document.getElementById('error-message');
        if (errorElement) {
            errorElement.style.display = 'none';
        }
    }

    hideMessages() {
        this.hideError();
        const successElement = document.getElementById('success-message');
        if (successElement) {
            successElement.style.display = 'none';
        }
    }

    startTokenRefresh() {
        // Clear existing timer
        if (this.refreshTimer) {
            clearInterval(this.refreshTimer);
        }

        // Refresh token every 14 minutes (before 15-minute expiry)
        this.refreshTimer = setInterval(async () => {
            try {
                const response = await fetch(`${this.apiBase}/auth/refresh`, {
                    method: 'POST',
                    credentials: 'include'
                });

                if (!response.ok) {
                    console.warn('Token refresh failed, redirecting to login');
                    this.logout();
                }
            } catch (error) {
                console.warn('Token refresh error:', error);
                this.logout();
            }
        }, 14 * 60 * 1000); // 14 minutes
    }

    async logout() {
        try {
            await fetch(`${this.apiBase}/auth/logout`, {
                method: 'POST',
                credentials: 'include'
            });
        } catch (error) {
            console.warn('Logout request failed:', error);
        } finally {
            this.currentUser = null;
            if (this.refreshTimer) {
                clearInterval(this.refreshTimer);
                this.refreshTimer = null;
            }
            window.location.href = '/login';
        }
    }

    redirectToDashboard() {
        window.location.href = '/dashboard';
    }

    // Public API for other parts of the app
    getCurrentUser() {
        return this.currentUser;
    }

    isAuthenticated() {
        return this.currentUser !== null;
    }
}

// Initialize auth manager when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.authManager = new AuthManager();
});

// Export for use in other scripts
if (typeof module !== 'undefined' && module.exports) {
    module.exports = AuthManager;
}

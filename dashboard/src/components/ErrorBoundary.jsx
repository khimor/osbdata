import { Component } from 'react';

export default class ErrorBoundary extends Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }

  componentDidCatch(error, info) {
    console.error('Dashboard error:', error, info);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div style={{
          display: 'flex', flexDirection: 'column', alignItems: 'center',
          justifyContent: 'center', height: '100vh', background: 'var(--bg-root)',
          color: 'var(--text-primary)', fontFamily: 'var(--font-body)',
        }}>
          <h2 style={{ fontSize: 20, marginBottom: 8 }}>Something went wrong</h2>
          <p style={{ color: 'var(--text-tertiary)', fontSize: 14, marginBottom: 16 }}>
            {this.state.error?.message || 'An unexpected error occurred'}
          </p>
          <button
            className="btn"
            onClick={() => { this.setState({ hasError: false, error: null }); window.location.reload(); }}
          >
            Reload
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}

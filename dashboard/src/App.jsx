import { useState, useCallback, useMemo } from 'react';
import ErrorBoundary from './components/ErrorBoundary';
import Sidebar from './components/Sidebar';
import NationalOverview from './components/NationalOverview';
import OperatorView from './components/OperatorView';
import StateComparison from './components/StateComparison';
import StateDeepDive from './components/StateDeepDive';
import DataTable from './components/DataTable';

export default function App() {
  const [activeView, setActiveView] = useState('national');
  const [selectedState, setSelectedState] = useState('NY');

  const handleNavigateToState = useCallback((stateCode) => {
    setSelectedState(stateCode);
    setActiveView('state');
  }, []);

  const dataAsOf = useMemo(() => {
    const d = new Date();
    const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    return `${months[d.getMonth()]} ${d.getDate()}, ${d.getFullYear()}`;
  }, []);

  return (
    <ErrorBoundary>
      <div className="app-layout">
        <Sidebar activeView={activeView} onNavigate={setActiveView} dataAsOf={dataAsOf} />
        <main className="main-content" role="main" aria-label="Dashboard content">
          <ErrorBoundary>
            {activeView === 'national' && (
              <NationalOverview onNavigateToState={handleNavigateToState} />
            )}
            {activeView === 'operators' && (
              <OperatorView />
            )}
            {activeView === 'compare' && (
              <StateComparison />
            )}
            {activeView === 'state' && (
              <StateDeepDive stateCode={selectedState} />
            )}
            {activeView === 'data' && (
              <DataTable />
            )}
          </ErrorBoundary>
        </main>
      </div>
    </ErrorBoundary>
  );
}

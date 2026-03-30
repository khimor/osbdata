import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { BrowserRouter, Routes, Route, Navigate, useParams } from 'react-router-dom';
import { Analytics } from '@vercel/analytics/react';
import App from './App';
import LandingPage from './components/LandingPage';
import ApiAccessPage from './components/ApiAccessPage';
import './styles.css';

function StateRedirect() {
  const { code } = useParams();
  return <Navigate to="/app" state={{ view: 'state', stateCode: code.toUpperCase() }} replace />;
}

function OperatorRedirect() {
  return <Navigate to="/app" state={{ view: 'operators' }} replace />;
}

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<LandingPage />} />
        <Route path="/api-access" element={<ApiAccessPage />} />
        <Route path="/state/:code" element={<StateRedirect />} />
        <Route path="/operators" element={<OperatorRedirect />} />
        <Route path="/app/*" element={<App />} />
      </Routes>
    </BrowserRouter>
    <Analytics />
  </StrictMode>
);

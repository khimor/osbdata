import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { Analytics } from '@vercel/analytics/react';
import App from './App';
import LandingPage from './components/LandingPage';
import ApiAccessPage from './components/ApiAccessPage';
import './styles.css';

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<LandingPage />} />
        <Route path="/api-access" element={<ApiAccessPage />} />
        <Route path="/app/*" element={<App />} />
      </Routes>
    </BrowserRouter>
    <Analytics />
  </StrictMode>
);

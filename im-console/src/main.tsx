import React from 'react';
import ReactDOM from 'react-dom/client';
import { createBrowserRouter, RouterProvider } from 'react-router-dom';
import App from './App';
import LoginPage from './pages/LoginPage';
import ConsolePage from './pages/ConsolePage';
import MessageDetailPage from './pages/MessageDetailPage';
import MonitorPage from './pages/MonitorPage';
import './styles/design.css';

const router = createBrowserRouter([
  { path: '/login', element: <LoginPage /> },
  {
    path: '/',
    element: <App />,
    children: [
      { index: true, element: <ConsolePage /> },
      { path: 'console', element: <ConsolePage /> },
      { path: 'message/:messageId', element: <MessageDetailPage /> },
      { path: 'monitor', element: <MonitorPage /> },
    ],
  },
]);

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <RouterProvider router={router} />
  </React.StrictMode>,
);

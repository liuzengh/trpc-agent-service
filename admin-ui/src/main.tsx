import React from 'react'
import ReactDOM from 'react-dom/client'
import 'tdesign-react/es/style/index.css'
import './styles.css'
import { AdminApp } from './app'

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <AdminApp />
  </React.StrictMode>,
)

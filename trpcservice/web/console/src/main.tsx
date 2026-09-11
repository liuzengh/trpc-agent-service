import React from "react";
import ReactDOM from "react-dom/client";
import { App as AntApp, ConfigProvider } from "antd";
import zhCN from "antd/locale/zh_CN";
import { StyleProvider } from "@ant-design/cssinjs";
import "antd/dist/antd.css";
import "./style.css";
import { ConsoleApp } from "./App";

const nonce = document.querySelector<HTMLMetaElement>(
  'meta[name="csp-nonce"]',
)?.content;
ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <StyleProvider>
      <ConfigProvider
        locale={zhCN}
        csp={{ nonce }}
        theme={{
          zeroRuntime: true,
          token: {
            colorPrimary: "#7161d9",
            borderRadius: 9,
            fontFamily:
              'Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", sans-serif',
            colorText: "#252637",
            colorBgLayout: "#f6f7fb",
          },
        }}
      >
        <AntApp>
          <ConsoleApp />
        </AntApp>
      </ConfigProvider>
    </StyleProvider>
  </React.StrictMode>,
);

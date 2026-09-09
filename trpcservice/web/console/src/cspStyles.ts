// Some rc-util/Portal styles do not forward ConfigProvider's nonce (scrollbar
// measurements and modal scroll locks). Adapt the dependency's CSS entrypoints,
// keeping its removal/cache semantics and without patching global DOM APIs.
import {
  updateCSS as originalUpdateCSS,
  injectCSS as originalInjectCSS,
} from "@rc-component/util/es/Dom/dynamicCSS";
export * from "@rc-component/util/es/Dom/dynamicCSS";

function optionsWithNonce(
  options: Parameters<typeof originalUpdateCSS>[2] = {},
) {
  const nonce = document.querySelector<HTMLMetaElement>(
    'meta[name="csp-nonce"]',
  )?.content;
  return {
    ...options,
    csp: { ...options.csp, nonce: options.csp?.nonce || nonce },
  };
}

export function injectCSS(
  css: string,
  options: Parameters<typeof originalInjectCSS>[1] = {},
) {
  return originalInjectCSS(css, optionsWithNonce(options));
}

export function updateCSS(
  css: string,
  key: string,
  options: Parameters<typeof originalUpdateCSS>[2] = {},
) {
  return originalUpdateCSS(css, key, optionsWithNonce(options));
}

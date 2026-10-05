// Build-time configuration. On AWS Amplify these come from the app's environment
// variables, which the backend SAM template sets from its own resources.
export const config = {
  region: import.meta.env.VITE_AWS_REGION,
  apiUrl: (import.meta.env.VITE_API_URL || "").replace(/\/$/, ""),
  userPoolId: import.meta.env.VITE_USER_POOL_ID,
  userPoolClientId: import.meta.env.VITE_USER_POOL_CLIENT_ID,
};

export const missingConfig = Object.entries(config)
  .filter(([, value]) => !value)
  .map(([key]) => key);

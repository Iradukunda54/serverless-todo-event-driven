// Calls API Gateway with the Cognito ID token (the REST API's Cognito authorizer
// validates ID tokens).
import { fetchAuthSession } from "aws-amplify/auth";
import { config } from "./config.js";

async function request(method, path, body) {
  const { tokens } = await fetchAuthSession();
  if (!tokens?.idToken) throw new Error("Not signed in");

  const res = await fetch(`${config.apiUrl}${path}`, {
    method,
    headers: {
      Authorization: tokens.idToken.toString(),
      ...(body ? { "Content-Type": "application/json" } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  if (res.status === 204) return null;
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.message || `Request failed (${res.status})`);
  return data;
}

export const listTasks = () => request("GET", "/tasks").then((d) => d.tasks);
export const createTask = (task) => request("POST", "/tasks", task);
export const updateTask = (id, changes) => request("PUT", `/tasks/${encodeURIComponent(id)}`, changes);
export const deleteTask = (id) => request("DELETE", `/tasks/${encodeURIComponent(id)}`);

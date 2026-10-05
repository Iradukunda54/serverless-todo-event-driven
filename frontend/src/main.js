import { Amplify } from "aws-amplify";
import { fetchUserAttributes, getCurrentUser, signIn, signOut, signUp } from "aws-amplify/auth";
import { createTask, deleteTask, listTasks, updateTask } from "./api.js";
import { config, missingConfig } from "./config.js";
import "./style.css";

const REFRESH_MS = 15000;
const $ = (id) => document.getElementById(id);

let authMode = "signin";
let refreshTimer = null;
// Server clock minus browser clock, learned from a created task's CreatedAt, so the
// countdown stays correct even when the device clock is off (deadlines are server-side).
let clockOffsetMs = 0;
const serverNow = () => Date.now() + clockOffsetMs;

Amplify.configure({
  Auth: {
    Cognito: {
      userPoolId: config.userPoolId,
      userPoolClientId: config.userPoolClientId,
      loginWith: { email: true },
    },
  },
});

// ------------------------------------------------------------ UI helpers
function showBanner(message, kind = "error") {
  const banner = $("banner");
  banner.textContent = message;
  banner.className = `banner ${kind}`;
  banner.hidden = !message;
}

const today = () => new Date(serverNow()).toISOString().slice(0, 10);

function formatDeadline(task) {
  const deadline = new Date(task.Deadline);
  const local = deadline.toLocaleString();
  if (task.Status !== "Pending") return `Deadline ${local}`;
  const seconds = Math.round((deadline - serverNow()) / 1000);
  if (seconds <= 0) return `Deadline ${local} (expiring…)`;
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return `Expires in ${m}m ${String(s).padStart(2, "0")}s (${local})`;
}

// ------------------------------------------------------------ Auth
function setAuthMode(mode) {
  authMode = mode;
  document.querySelectorAll(".tab").forEach((t) => t.classList.toggle("active", t.dataset.mode === mode));
  $("auth-submit").textContent = mode === "signin" ? "Sign in" : "Create account";
  $("password-hint").hidden = mode === "signin";
  $("auth-password").autocomplete = mode === "signin" ? "current-password" : "new-password";
}

async function handleAuth(event) {
  event.preventDefault();
  showBanner("");
  const email = $("auth-email").value.trim();
  const password = $("auth-password").value;
  $("auth-submit").disabled = true;
  try {
    if (authMode === "signup") {
      // The PreSignUp trigger auto-confirms the account, so we can sign in straight away.
      await signUp({ username: email, password, options: { userAttributes: { email } } });
    }
    const result = await signIn({ username: email, password });
    if (!result.isSignedIn) throw new Error(`Additional sign-in step required: ${result.nextStep?.signInStep}`);
    await enterApp();
  } catch (err) {
    showBanner(err.message || String(err));
  } finally {
    $("auth-submit").disabled = false;
  }
}

async function enterApp() {
  const attributes = await fetchUserAttributes();
  $("user-email").textContent = attributes.email;
  $("session").hidden = false;
  $("auth-view").hidden = true;
  $("tasks-view").hidden = false;
  $("new-date").value = today();
  await refresh();
  clearInterval(refreshTimer);
  refreshTimer = setInterval(refresh, REFRESH_MS);
}

async function leaveApp() {
  clearInterval(refreshTimer);
  await signOut();
  $("session").hidden = true;
  $("tasks-view").hidden = true;
  $("auth-view").hidden = false;
}

// ------------------------------------------------------------ Tasks
function renderTasks(tasks) {
  const template = $("task-template");
  document.querySelectorAll(".column").forEach((column) => {
    const status = column.dataset.status;
    const list = column.querySelector("ul");
    const items = tasks.filter((t) => t.Status === status);
    column.querySelector(".count").textContent = `(${items.length})`;
    list.replaceChildren(
      ...items.map((task) => {
        const node = template.content.firstElementChild.cloneNode(true);
        node.classList.add(status.toLowerCase());
        node.querySelector(".description").textContent = task.Description;
        node.querySelector(".meta").textContent = `${task.Date} · ${formatDeadline(task)}`;
        node.querySelector(".complete").hidden = status !== "Pending";
        node.querySelector(".complete").onclick = () => run(() => updateTask(task.TaskId, { Status: "Completed" }));
        node.querySelector(".edit").onclick = () => editTask(task);
        node.querySelector(".delete").onclick = () => {
          if (confirm(`Delete "${task.Description}"?`)) run(() => deleteTask(task.TaskId));
        };
        return node;
      }),
    );
  });
}

async function refresh() {
  try {
    renderTasks(await listTasks());
    $("last-refresh").textContent = `Updated ${new Date().toLocaleTimeString()} · auto-refresh every 15s`;
  } catch (err) {
    showBanner(err.message || String(err));
  }
}

async function run(action) {
  showBanner("");
  try {
    await action();
  } catch (err) {
    showBanner(err.message || String(err));
  }
  await refresh();
}

function editTask(task) {
  const description = prompt("Description", task.Description);
  if (description === null) return;
  const date = prompt("Date (YYYY-MM-DD)", task.Date);
  if (date === null) return;
  run(() => updateTask(task.TaskId, { Description: description, Date: date }));
}

async function handleCreate(event) {
  event.preventDefault();
  const minutes = $("new-deadline").value;
  const task = { Description: $("new-description").value, Date: $("new-date").value };
  // Relative value: the server computes the deadline with its own clock.
  if (minutes) task.ExpiresInMinutes = Number(minutes);
  await run(async () => {
    const sentAt = Date.now();
    const created = await createTask(task);
    clockOffsetMs = Date.parse(created.CreatedAt) - (sentAt + Date.now()) / 2;
    $("new-description").value = "";
  });
}

// ------------------------------------------------------------ Boot
document.querySelectorAll(".tab").forEach((tab) => (tab.onclick = () => setAuthMode(tab.dataset.mode)));
$("auth-form").addEventListener("submit", handleAuth);
$("create-form").addEventListener("submit", handleCreate);
$("sign-out").onclick = leaveApp;
$("refresh").onclick = refresh;

if (missingConfig.length) {
  showBanner(`Missing configuration: ${missingConfig.join(", ")}. See frontend/.env.example.`);
}

getCurrentUser()
  .then(enterApp)
  .catch(() => {
    $("auth-view").hidden = false;
  });

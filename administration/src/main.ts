import { mount } from "svelte";
import App from "./App.svelte";
import "@fontsource-variable/source-sans-3";
import "./style.css";
mount(App, { target: document.getElementById("app")! });

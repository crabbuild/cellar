// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Node deliberately uses independently generated plain JavaScript bindings.
import { Echo, handler } from "./app.js";
import { ProfileClient, ProfileRegistration } from "../fixtures/profile-generated/javascript/profile_cellar.js";

export const cells = {NodeCounter: Echo};
export const rpc = {NodeCounter: ProfileRegistration};
export const fetch = handler(ProfileClient);

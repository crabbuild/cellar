// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { Echo, handler } from "./app.js";
import { ProfileClient, ProfileRegistration } from "../fixtures/profile-generated/typescript/profile_cellar.js";

export const cells = {V8Counter: Echo};
export const rpc = {V8Counter: ProfileRegistration};
export const fetch = handler(ProfileClient);

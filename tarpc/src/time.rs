// Copyright 2018 Google LLC
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! Cross-platform monotonic time types.

#[cfg(not(target_family = "wasm"))]
pub(crate) use std::time::Instant;
#[cfg(target_family = "wasm")]
pub(crate) use web_time::Instant;

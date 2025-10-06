//! Manifest types for external tool operations.
//!
//! This module is separated from other tool modules to prevent type definition
//! clashes that can occur when typify generates types from the same JSON schema
//! in multiple modules. By isolating the schema imports here, we ensure that
//! the generated types are only defined once and can be safely imported by
//! other modules that need them.

use typify::import_types;

import_types!(
    schema = "models/admin/tools/manifest.json",
    struct_builder = true,
);

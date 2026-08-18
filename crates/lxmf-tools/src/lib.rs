//! LXMF Tools: shared library code for lxmd and LXMF CLI utilities.

pub mod config;
pub mod daemon;
pub mod lxmd_cli;
pub mod lxmd_control;
pub mod lxmd_runtime;

#[cfg(test)]
mod storage_api_guards {
    use std::path::PathBuf;

    #[test]
    fn production_code_does_not_access_router_storage_fields() {
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let sources = [
            manifest.join("src/daemon.rs"),
            manifest.join("src/lxmd_control.rs"),
            manifest.join("src/commands/lxmd.rs"),
        ];
        let forbidden = [
            "router.pending_outbound",
            "router.pending_deferred_stamps",
            "router.propagation_store",
            "router.outbound_stamp_costs",
            "router.ticket_store",
            "router.peers",
            "router.static_peers",
            "router.allowed_control",
            "router.throttled_peers",
        ];

        for path in sources {
            let source = std::fs::read_to_string(&path).expect("read lxmf-tools source");
            let production = source.split("#[cfg(test)]").next().unwrap_or(&source);
            for field in forbidden {
                assert!(
                    !production.contains(field),
                    "{} accesses storage-sensitive field `{field}` directly",
                    path.display()
                );
            }
        }
    }
}

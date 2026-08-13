//! Compile-time guard for the application-facing API that must survive the
//! SQLite migration. The functions are intentionally not executed: compiling
//! their bodies verifies names, ownership and the important method shapes.

use lxmf_core::application::{DELIVERY_APP_NAME, DeliveryIdentity};
use lxmf_core::constants::{DeliveryMethod, UnverifiedReason};
use lxmf_core::message::{LxMessage, MessageError};
use lxmf_core::propagation_client::{PropagationClient, PropagationClientState};
use lxmf_core::router::{LxmRouter, RouterConfig};
use lxmf_core::types::{DestinationHash, MessageId, PropagationTransientId};

#[allow(dead_code)]
fn application_api(
    delivery: &mut DeliveryIdentity,
    message: &mut LxMessage,
) -> Result<(), MessageError> {
    let _: &str = DELIVERY_APP_NAME;
    let _: DestinationHash = delivery.destination_hash();
    let _ = delivery.identity();
    let _ = delivery.destination();
    let _ = delivery.display_name();
    delivery.set_display_name(Some("node".to_string()));
    let _ = delivery.stamp_cost();
    let _: Vec<u8> = delivery.announce_app_data();

    message.set_field(0x01, vec![1, 2, 3]);
    let _ = message.get_field(0x01);
    let _: Vec<u8> = message.pack_payload()?;
    message.compute_hash()?;
    let _: Option<MessageId> = message.message_id;
    let _: Option<PropagationTransientId> = message.transient_id;
    Ok(())
}

#[allow(dead_code)]
fn propagation_client_api(client: &mut PropagationClient) {
    let _: PropagationClientState = client.state;
    client.set_propagation_node([0; 16]);
    let _: bool = client.start_download();
    let _: Vec<Vec<u8>> = client.take_received_messages();
}

#[allow(dead_code)]
fn router_api(router: &mut LxmRouter, message: LxMessage) {
    router.send(message);
    let _ = router.stats();
    let _ = router.outbound_summaries(16);
    let _ = router.propagation_metadata_page(None, 16);
    let _ = router.control_status();
}

#[test]
fn stable_types_remain_public() {
    fn assert_public<T>() {}

    assert_public::<DeliveryIdentity>();
    assert_public::<LxMessage>();
    assert_public::<MessageError>();
    assert_public::<DeliveryMethod>();
    assert_public::<UnverifiedReason>();
    assert_public::<PropagationClient>();
    assert_public::<PropagationClientState>();
    assert_public::<LxmRouter>();
    assert_public::<RouterConfig>();
}

use super::*;

#[test]
fn consumer_only_sources_receive_native_codec_inventory() {
    let contract = Arc::new(
        Contract::from_descriptor(
            include_bytes!("../../tools/rpc/fixtures/counter.binpb"),
            cellar::sdk::rpc::Limits::default(),
        )
        .unwrap(),
    );
    let source = Source::new("", "consumer.js")
        .rpc_contract(contract.clone())
        .unwrap();
    let descriptor = source.descriptor();
    assert!(descriptor["rpc"].is_null());
    assert_eq!(
        descriptor["rpc_contracts"][contract.digest()],
        contract.inventory()
    );
}

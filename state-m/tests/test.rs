use state_m::*;

#[tokio::test]
async fn test() -> anyhow::Result<()> {
    on_change!((3, ""), |s| s + 1)
}

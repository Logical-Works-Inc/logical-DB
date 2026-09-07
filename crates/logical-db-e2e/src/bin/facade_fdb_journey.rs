//! Retained black-box journey: public facade -> HTTP -> Braid -> `FoundationDB`.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

use std::{error::Error, fs, io};

use logical_db::{
    AccountId, Database, DocumentId, Item, Object, OperationDigest, OperationId, PeerId, ReplicaId,
    Scalar, Scope, SpaceId, SyncLimits,
    http::{HttpTransport, StaticBearer, Url},
};
use uuid::Uuid;

fn required(name: &'static str) -> Result<String, io::Error> {
    std::env::var(name).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} is unavailable: {error}"),
        )
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let endpoint: Url = required("LOGICAL_DB_JOURNEY_ENDPOINT")?.parse()?;
    let bearer_token = required("LOGICAL_DB_BEARER_TOKEN")?;
    let bearer = StaticBearer::new(&bearer_token)?;
    let scope = Scope::new(
        AccountId::from_uuid(Uuid::parse_str(&required("LOGICAL_DB_ACCOUNT_ID")?)?),
        SpaceId::from_uuid(Uuid::parse_str(&required("LOGICAL_DB_SPACE_ID")?)?),
    );
    let run = Uuid::now_v7();
    let first_path = std::env::temp_dir().join(format!("logical-db-fdb-first-{run}.redb"));
    let second_path = std::env::temp_dir().join(format!("logical-db-fdb-second-{run}.redb"));
    let first = Database::open(&first_path, scope, ReplicaId::new())?;
    let second = Database::open(&second_path, scope, ReplicaId::new())?;
    let document = DocumentId::new();
    let peer = PeerId::new();
    let mut transport = HttpTransport::new(endpoint, bearer)?;

    first.edit(
        document,
        OperationId::new(),
        OperationDigest::new([0x41; 32]),
        |editor| {
            editor.put(
                &Object::root(),
                "verified",
                Scalar::String("facade-http-braid-fdb".into()),
            )
        },
    )?;
    let upload = first
        .synchronize(peer, &mut transport, SyncLimits::default(), || false)
        .await?;
    let download = second
        .synchronize(peer, &mut transport, SyncLimits::default(), || false)
        .await?;
    let observed = second.read(document, |reader| reader.get(&Object::root(), "verified"))?;
    if observed
        != Some(Some(Item::Scalar(Scalar::String(
            "facade-http-braid-fdb".into(),
        ))))
    {
        eprintln!("facade-fdb-journey: replica did not materialize verified state");
        return Err(io::Error::other("replica did not materialize verified state").into());
    }
    let retry = first
        .synchronize(peer, &mut transport, SyncLimits::default(), || false)
        .await?;
    if upload.pushed != 1 || download.pulled != 1 || retry.pushed != 0 {
        eprintln!(
            "facade-fdb-journey: unexpected counts pushed={} pulled={} retry={}",
            upload.pushed, download.pulled, retry.pushed
        );
        return Err(io::Error::other("journey report violated exact retry semantics").into());
    }

    drop((first, second));
    fs::remove_file(&first_path)?;
    fs::remove_file(&second_path)?;
    println!(
        "PASS facade-http-braid-fdb pushed={} pulled={} retry={}",
        upload.pushed, download.pulled, retry.pushed
    );
    Ok(())
}

use anyhow::{Context, Result};
use chrono::TimeZone;
use futures::StreamExt;
use std::collections::VecDeque;
use std::io::Write;
use ton_block::{CommonMsgInfo, GetRepresentationHash, Serializable};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::SubscriberBuilder::default()
        .with_max_level(tracing::Level::INFO)
        .init();
    let hashes = std::fs::read_to_string(
        "/home/odm3n/Datasets/tycho/graphs/87db6943508354e1508cff0c5e1df4dc2ef0a29a/hashes.log",
    )
    .unwrap();
    let client = everscale_rpc_client::RpcClient::new(
        vec!["http://57.128.117.6:8081/rpc".parse().unwrap()],
        everscale_rpc_client::ClientOptions {
            request_timeout: std::time::Duration::from_secs(30),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let dst =
        hex::decode("1f541e41c01136a416676cb016c7ffc55e0af721e461e80298e020fed6e4912e").unwrap();
    let dst = client.get_dst_transaction(&dst).await.unwrap().unwrap();
    let inm = dst.in_msg_cell().unwrap();
    println!("{}", inm.write_to_bytes().unwrap().len());
    return;

    // let depth = get_dst_hashes(
    //     &client,
    //     &hex::decode("4df3353828e373abeb72317fd2eb4d847d7619206d01763ec217752b5d31d871").unwrap(),
    // )
    // .await
    // .unwrap();
    // println!("Depth: {depth}");
    // return;

    let total = hashes.lines().count();
    let mut stream = futures::stream::iter(hashes.lines())
        .map(|(hash)| {
            let client = client.clone();
            async move {
                let depth = get_dst_hashes(&client, &hex::decode(hash).unwrap())
                    .await
                    .unwrap();
                (hash, depth)
            }
        })
        .buffer_unordered(4000)
        .enumerate();
    let mut stream = std::pin::pin!(stream);

    let mut file = std::fs::File::create("depths.log").unwrap();
    writeln!(file, "hash,depth,start_time,end_time,total_time").unwrap();
    while let Some((idx, (hash, depth))) = stream.next().await {
        if idx % 100 == 0 {
            println!("Processed hash {idx}/{total}: {hash} len: {}", depth.depth);
        }
        if depth.depth == 0 {
            continue;
        }
        let Stat {
            depth,
            start_time,
            end_time,
            total_time,
        } = depth;
        writeln!(
            file,
            "{hash},{depth},{start_time},{end_time},{}",
            total_time.num_seconds()
        )
        .unwrap();
    }
    file.flush().unwrap();
}

async fn get_dst_hashes(
    client: &everscale_rpc_client::RpcClient,
    initial_input: &[u8],
) -> Result<Stat> {
    let mut stat = Stat::default();
    let mut queue = VecDeque::new();
    queue.push_back(initial_input.to_vec());
    let mut is_first = true;

    while let Some(input) = queue.pop_front() {
        stat.depth += 1;
        let hex = hex::encode(&input);

        let result = client.get_dst_transaction(&input).await?;
        let result = match result {
            Some(result) => result,
            None => {
                if is_first {
                    return Ok(stat);
                }
                println!("No result for {hex}",);
                let mut file = std::fs::OpenOptions::new()
                    .append(true)
                    .create(true)
                    .write(true)
                    .open("no_result.log")
                    .unwrap();
                writeln!(file, "{hex}").unwrap();
                continue;
            }
        };
        if is_first {
            let time = result.now;
            stat.start_time = chrono::Utc.timestamp_opt(time as i64, 0).unwrap();
            is_first = false;
        }
        let time = result.now;
        stat.end_time = std::cmp::max(
            stat.end_time,
            chrono::Utc.timestamp_opt(time as i64, 0).unwrap(),
        );

        let mut out = Vec::new();
        result
            .iterate_out_msgs(|m| {
                out.push(m);
                Ok(true)
            })
            .context("Failed to iterate out messages")?;

        for msg in out {
            if let CommonMsgInfo::IntMsgInfo(_) = msg.header() {
                if let Ok(hash) = msg.hash() {
                    let hash_inner = hash.inner();
                    queue.push_back(hash_inner.to_vec());
                }
            }
        }
    }
    stat.update_time();

    Ok(stat)
}

#[derive(Default)]
struct Stat {
    depth: u32,
    start_time: chrono::DateTime<chrono::Utc>,
    end_time: chrono::DateTime<chrono::Utc>,
    total_time: chrono::Duration,
}

impl Stat {
    fn update_time(&mut self) {
        self.total_time = self.end_time - self.start_time;
    }
}

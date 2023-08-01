use anyhow::{anyhow, Result};
use regex::Regex;
use std::env;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{tcp::ReadHalf, TcpListener},
};

const SOH: char = '\u{0001}';

async fn read_fix(reader: &mut BufReader<ReadHalf<'_>>) -> Result<String> {
    let mut buffer = String::new();
    let regex = Regex::new(r"10=([0-9]{3})").unwrap();
    loop {
        let mut buf = Vec::<u8>::new();
        let bytes_read = reader.read_until(SOH as u8, &mut buf).await.unwrap();
        buffer += String::from_utf8(buf).unwrap().trim();
        if regex.find(&buffer).is_some() {
            // Loop until full FIX message received
            break;
        };
        if bytes_read == 0 {
            return Err(anyhow!("client disconnected"));
        }
    }
    for capture in regex.captures_iter(&buffer) {
        let check_sum_removed = regex.replace(&buffer, "");
        let byte_sum: usize = check_sum_removed
            .as_bytes()
            .iter()
            .map(|&x| x as usize)
            .sum();
        let checksum = capture[1].parse::<usize>().unwrap();
        if checksum != byte_sum % 256_usize {
            println!("bad checksum");
        }
    }
    println!("{}", buffer.replace(SOH, "|"));
    Ok(buffer)
}
async fn get_msg_type(msg: String) -> String {
    let regex = Regex::new(r"35=([^]+)").unwrap();
    let mut msg_type = String::new();
    for capture in regex.captures_iter(&msg) {
        msg_type = capture[1].to_string();
    }
    msg_type
}

async fn acceptor() -> Result<()> {
    let md_listener = TcpListener::bind("localhost:8080").await?;
    let or_listener = TcpListener::bind("localhost:8081").await?;

    loop {
        tokio::select! {
            md_socket = md_listener.accept() => {
                let mut md_socket = md_socket.unwrap().0;
                tokio::spawn(async move {
                    let (md_read, md_write) = md_socket.split();
                    let mut md_reader = BufReader::new(md_read);
                    let mut md_writer = BufWriter::new(md_write);
                    loop {
                        let Ok(msg) = read_fix(&mut md_reader).await else {
                            break;
                        };
                        let msg_type = get_msg_type(msg).await;

                        if msg_type == "A" {
                            md_writer.write_all(b"8=FIX.4.49=10235=A49=SENDERMD56=TARGETMD34=152=20190605-11:40:30.39298=0108=30141=Y553=user554=password10=104\n").await.expect("failed to send logon");
                        } else if msg_type == "V" {
                            md_writer.write_all(b"8=FIX.4.49=31335=W49=SENDERMD56=TARGETMD34=152=20230427-10:30:00.12355=EURUSD268=2269=0270=175.10271=500272=20230427273=10:30:00.123269=1270=1255.20271=200272=20230427273=10:30:00.12310=057").await.unwrap();
                        }
                        md_writer.flush().await.unwrap();
                    }
                });
            }
            or_socket = or_listener.accept() => {
                let mut or_socket = or_socket.unwrap().0;
                tokio::spawn(async move {
                    let (or_read, or_write) = or_socket.split();
                    let mut or_reader = BufReader::new(or_read);
                    let mut or_writer = BufWriter::new(or_write);
                    loop {
                        let Ok(msg) = read_fix(&mut or_reader).await else {
                            break;
                        };
                        let msg_type = get_msg_type(msg).await;

                        if msg_type == "A" {
                            or_writer.write_all(b"8=FIX.4.49=10235=A49=SENDEROR56=TARGETOR34=152=20190605-11:40:30.39298=0108=30141=Y553=Username554=Password10=104\n").await.expect("failed to send logon");
                        } else if msg_type == "D" {
                            or_writer.write_all(b"8=FIX.4.49=31335=849=SENDEROR56=TARGETOR34=152=20230427-10:30:00.12355=AAPL268=1269=0270=175.10271=500272=20230427273=10:30:00.123279=055=GOOG268=2269=1270=1255.20271=200272=20230427273=10:30:00.123279=110=057").await.unwrap();
                        }
                        or_writer.flush().await.unwrap();
                    }
                });
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() == 1 {
        println!("no argument provided");
        return Ok(());
    }
    if args[1] == "acceptor" {
        acceptor().await?;
    } else {
        return Err(anyhow!("invalid argument provided"));
    }
    Ok(())
}

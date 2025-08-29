#![allow(unused_imports)]
use std::net::TcpListener;
use std::io::Read;
use std::io::Write;
use std::thread;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                thread::spawn(move || handle_connection(stream));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_connection(mut stream: std::net::TcpStream) {
    let mut buffer = [0; 512];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break, // Connection closed
            Ok(n) => {
                // if stream.write(b"+PONG\r\n").is_err() || stream.flush().is_err() {
                //     break;
                // }
                let input = String::from_utf8_lossy(&buffer[..n]);
                println!("Received: {:?}", input);

                match input.chars().next() {
                    Some('*') => {
                        let args = parse_rp_array(&input);
                        if args.is_empty() {
                            if stream.write(b"-ERR unknown command\r\n").is_err() || stream.flush().is_err() {
                                break;
                            }
                            continue;
                        }
                        match args[0].to_uppercase().as_str() {
                            "PING" => {
                                if stream.write(b"+PONG\r\n").is_err() || stream.flush().is_err() {
                                    break;
                                }
                            }
                            "ECHO" => {
                                if args.len() < 2 {
                                    if stream.write(b"-ERR wrong number of arguments for 'echo' command\r\n").is_err() || stream.flush().is_err() {
                                        break;
                                    }
                                    continue;
                                }
                                let response = format!("${}\r\n{}\r\n", args[1].len(), args[1]);
                                if stream.write(response.as_bytes()).is_err() || stream.flush().is_err() {
                                    break;
                                }
                            }
                            _ => {
                                if stream.write(b"-ERR unknown command\r\n").is_err() || stream.flush().is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    _ => {
                        if stream.write(b"-ERR unknown command format\r\n").is_err() || stream.flush().is_err() {
                            break;
                        }
                    }
                }
            }
            Err(_) => break,
        }
    }
    println!("Connection closed");
}

fn parse_rp_array(input: &str) -> Vec<String> {
    let mut elements = input.split("\r\n");
    let mut result = Vec::new();
    let mut count = 0;
    while let Some(element) = elements.next() {
        if element.starts_with('*') {
            count = element[1..].parse::<usize>().unwrap_or(0);
        } else if element.starts_with('$') {
            let len = element[1..].parse::<usize>().unwrap_or(0);
            let blk_str = elements.next().unwrap_or("");
            if blk_str.len() != len {
                break;
            }
            result.push(blk_str.to_string());
        }
    }
    if result.len() != count {
        result.clear();
    }
    result
}

#![allow(unused_imports)]
use std::net::TcpListener;
use std::io::Read;
use std::io::Write;
use std::thread;
use std::collections::HashMap;
use std::sync::Mutex;
use lazy_static::lazy_static;

#[derive(Debug)]
enum ParseError {
    InvalidFormat,
    InvalidLength,
    MalformedInput,
    UnexpectedEnd,
}

#[derive(Debug)]
enum RedisValue {
    Array(Vec<RedisValue>),
    BulkString(String),
    SimpleString(String),
    Error(String),
}

lazy_static! {
    static ref STORE: Mutex<HashMap<String, String>> = Mutex::new(HashMap::new());
}

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
                let input = String::from_utf8_lossy(&buffer[..n]);
                println!("Received: {:?}", input);

                match parse_redis_protocol(&input) {
                    Ok(RedisValue::Array(args)) => {
                        if let Err(_) = handle_redis_command(&mut stream, &args) {
                            break;
                        }
                    }
                    Ok(_) => {
                        if send_error(&mut stream, "ERR unexpected command format").is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        println!("Parse error: {:?}", e);
                        if send_error(&mut stream, "ERR protocol error").is_err() {
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

fn parse_redis_protocol(input: &str) -> Result<RedisValue, ParseError> {
    let mut lines = input.split("\r\n").peekable();
    
    match lines.peek() {
        Some(line) if line.starts_with('*') => parse_array(&mut lines),
        Some(line) if line.starts_with('$') => parse_bulk_string(&mut lines),
        Some(line) if line.starts_with('+') => parse_simple_string(&mut lines),
        Some(line) if line.starts_with('-') => parse_error_string(&mut lines),
        _ => Err(ParseError::InvalidFormat),
    }
}

fn parse_array(lines: &mut std::iter::Peekable<std::str::Split<&str>>) -> Result<RedisValue, ParseError> {
    let first_line = lines.next().ok_or(ParseError::UnexpectedEnd)?;
    
    if !first_line.starts_with('*') {
        return Err(ParseError::InvalidFormat);
    }
    
    let count = first_line[1..].parse::<i32>()
        .map_err(|_| ParseError::InvalidLength)?;
    
    if count < 0 {
        return Err(ParseError::InvalidLength);
    }
    
    let mut elements = Vec::new();
    
    for _ in 0..count {
        match parse_bulk_string(lines) {
            Ok(RedisValue::BulkString(s)) => elements.push(RedisValue::BulkString(s)),
            Ok(_) => return Err(ParseError::InvalidFormat),
            Err(e) => return Err(e),
        }
    }
    
    Ok(RedisValue::Array(elements))
}

fn parse_bulk_string(lines: &mut std::iter::Peekable<std::str::Split<&str>>) -> Result<RedisValue, ParseError> {
    let length_line = lines.next().ok_or(ParseError::UnexpectedEnd)?;
    
    if !length_line.starts_with('$') {
        return Err(ParseError::InvalidFormat);
    }
    
    let length = length_line[1..].parse::<i32>()
        .map_err(|_| ParseError::InvalidLength)?;
    
    if length < 0 {
        return Err(ParseError::InvalidLength);
    }
    
    let content = lines.next().ok_or(ParseError::UnexpectedEnd)?;
    
    if content.len() != length as usize {
        return Err(ParseError::MalformedInput);
    }
    
    Ok(RedisValue::BulkString(content.to_string()))
}

fn parse_simple_string(lines: &mut std::iter::Peekable<std::str::Split<&str>>) -> Result<RedisValue, ParseError> {
    let line = lines.next().ok_or(ParseError::UnexpectedEnd)?;
    
    if !line.starts_with('+') {
        return Err(ParseError::InvalidFormat);
    }
    
    Ok(RedisValue::SimpleString(line[1..].to_string()))
}

fn parse_error_string(lines: &mut std::iter::Peekable<std::str::Split<&str>>) -> Result<RedisValue, ParseError> {
    let line = lines.next().ok_or(ParseError::UnexpectedEnd)?;
    
    if !line.starts_with('-') {
        return Err(ParseError::InvalidFormat);
    }
    
    Ok(RedisValue::Error(line[1..].to_string()))
}

fn handle_redis_command(stream: &mut std::net::TcpStream, args: &[RedisValue]) -> Result<(), std::io::Error> {
    if args.is_empty() {
        send_error(stream, "ERR unknown command")?;
        return Ok(());
    }
    
    let command = match &args[0] {
        RedisValue::BulkString(s) => s,
        _ => {
            send_error(stream, "ERR invalid command format")?;
            return Ok(());
        }
    };
    
    match command.to_uppercase().as_str() {
        "PING" => {
            stream.write_all(b"+PONG\r\n")?;
            stream.flush()?;
        }
        "ECHO" => {
            if args.len() < 2 {
                send_error(stream, "ERR wrong number of arguments for 'echo' command")?;
                return Ok(());
            }
            
            if let RedisValue::BulkString(message) = &args[1] {
                let response = format!("${}\r\n{}\r\n", message.len(), message);
                stream.write_all(response.as_bytes())?;
                stream.flush()?;
            } else {
                send_error(stream, "ERR invalid argument type")?;
            }
        }
        "SET" => {
            if args.len() < 3 {
                send_error(stream, "ERR wrong number of arguments for 'set' command")?;
                return Ok(());
            }

            if let (RedisValue::BulkString(key), RedisValue::BulkString(value)) = (&args[1], &args[2]) {
                if let Some(old_value) = STORE.lock().unwrap().insert(key.clone(), value.clone()) {
                    println!("Updated key: {}; Old value: {}; New value: {}", key, old_value, value);
                } else {
                    println!("Set new key: {}; New value: {}", key, value);
                }
                send_resp_simple_string(stream, "OK")?;
            } else {
                send_error(stream, "ERR invalid argument type")?;
            }
        }
        "GET" => {
            if args.len() < 2 {
                send_error(stream, "ERR wrong number of arguments for 'get' command")?;
                return Ok(());
            }

            if let RedisValue::BulkString(key) = &args[1] {
                let value = STORE.lock().unwrap().get(key).cloned();
                match value {
                    Some(value) => {
                        send_resp_bulk_string(stream, value)?;
                    }
                    None => {
                        // Send null bulk string
                        send_resp_bulk_string(stream, "".to_string())?;
                    }
                }
            } else {
                send_error(stream, "ERR invalid argument type")?;
            }
        }
        _ => {
            send_error(stream, "ERR unknown command")?;
        }
    }

    Ok(())
}

fn send_error(stream: &mut std::net::TcpStream, message: &str) -> Result<(), std::io::Error> {
    let response = format!("-{}\r\n", message);
    stream.write_all(response.as_bytes())?;
    stream.flush()?;
    Ok(())
}

fn send_resp_simple_string(stream: &mut std::net::TcpStream, message: &str) -> Result<(), std::io::Error> {
    let response = format!("+{}\r\n", message);
    stream.write_all(response.as_bytes())?;
    stream.flush()?;
    Ok(())
}

fn send_resp_bulk_string(stream: &mut std::net::TcpStream, message: String) -> Result<(), std::io::Error> {
    if message.is_empty() {
        stream.write_all(b"$-1\r\n")?;
        stream.flush()?;
        return Ok(());
    }
    let response = format!("${}\r\n{}\r\n", message.len(), message);
    stream.write_all(response.as_bytes())?;
    stream.flush()?;
    Ok(())
}

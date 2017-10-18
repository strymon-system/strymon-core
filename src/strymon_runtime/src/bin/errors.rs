error_chain!{
    foreign_links {
        Io(::std::io::Error);
        SerdeJson(::serde_json::Error);
    }
}

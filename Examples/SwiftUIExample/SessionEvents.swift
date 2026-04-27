import EventBus

struct UserLoggedIn: Event {
    let username: String
}

struct UserLoggedOut: Event {
    let timestamp: Date
}

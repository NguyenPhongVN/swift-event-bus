import SwiftUI
import EventBus

struct ContentView: View {
    @State private var username = ""
    @EventListener(UserLoggedIn.self) private var login
    @EventPublisher private var publish

    var body: some View {
        NavigationStack {
            VStack(spacing: 16) {
                TextField("Username", text: $username)
                    .textFieldStyle(.roundedBorder)
                    .onChangeOfEmitEvent(of: username) { value in
                        UserLoggedIn(username: value)
                    }

                Button("Publish Login") {
                    publish(UserLoggedIn(username: username))
                }

                Text(login.username ?? "No login event yet")
                    .font(.headline)
            }
            .padding()
            .onEventLifeCycle(UserLoggedOut.self) { event in
                print("logout at", event.timestamp)
            }
            .navigationTitle("EventBus Example")
        }
    }
}

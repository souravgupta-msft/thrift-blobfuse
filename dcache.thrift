namespace go dcache

// stripe object
struct Stripe {
    1: string id,
    2: i64 offset,
    3: i64 length,
    4: string hash,
    5: binary data
}

// Define the service with RPC methods
service StripeService {
    // check if the node is reachable
    void Ping()

    // Fetch a stripe from the node
    GetStripeResponse GetStripe(1: GetStripeRequest request)

    // Store a stripe on the node
    void PutStripe(1: Stripe stripe)

    // Delete a stripe from the node
    void RemoveStripe(1: string stripeID)
}

struct GetStripeRequest {
    1: string stripeID,
    2: binary data,
}

struct GetStripeResponse {
    1: i64 bytesRead,
    2: binary data,
    3: string hash
}
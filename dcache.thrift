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
    // Fetch a stripe from the node
    Stripe GetStripe(1: string stripeID)

    // Store a stripe on the node
    void PutStripe(1: Stripe stripe)

    // Delete a stripe from the node
    void RemoveStripe(1: string stripeID)
}

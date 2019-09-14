use super::*;

#[test]
fn delay_generator() {
    let delay_generator = generate_delay_generator(
        util::DEFAULT_CONNECT_SERIES_TRIES_BEFORE_COOL_DOWN,
        util::DEFAULT_CONNECT_DELAY,
        util::DEFAULT_CONNECT_SERIES_DELAY,
        util::DEFAULT_COOL_DOWN,
    );

    let client = Client::new(Vec::new());
    assert_eq!(
        delay_generator(Arc::clone(&client), 1, 3),
        util::DEFAULT_CONNECT_DELAY
    );
    assert_eq!(
        delay_generator(Arc::clone(&client), 2, 3),
        util::DEFAULT_CONNECT_DELAY
    );
    assert_eq!(
        delay_generator(Arc::clone(&client), 3, 3),
        util::DEFAULT_CONNECT_SERIES_DELAY
    );
    assert_eq!(
        delay_generator(Arc::clone(&client), 4, 3),
        util::DEFAULT_CONNECT_DELAY
    );
    assert_eq!(
        delay_generator(Arc::clone(&client), 5, 3),
        util::DEFAULT_CONNECT_DELAY
    );
    assert_eq!(
        delay_generator(Arc::clone(&client), 6, 3),
        util::DEFAULT_CONNECT_SERIES_DELAY
    );
    assert_eq!(
        delay_generator(Arc::clone(&client), 7, 3),
        util::DEFAULT_CONNECT_DELAY
    );
    assert_eq!(
        delay_generator(Arc::clone(&client), 8, 3),
        util::DEFAULT_CONNECT_DELAY
    );
    assert_eq!(
        delay_generator(Arc::clone(&client), 9, 3),
        util::DEFAULT_COOL_DOWN
    );
    assert_eq!(
        delay_generator(Arc::clone(&client), 10, 3),
        util::DEFAULT_CONNECT_DELAY
    );
    assert_eq!(
        delay_generator(Arc::clone(&client), 11, 3),
        util::DEFAULT_CONNECT_DELAY
    );
    assert_eq!(
        delay_generator(Arc::clone(&client), 12, 3),
        util::DEFAULT_CONNECT_SERIES_DELAY
    );
}

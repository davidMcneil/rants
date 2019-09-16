use super::*;

#[test]
fn delay_generator() {
    let delay_generator = generate_delay_generator(
        util::DEFAULT_CONNECT_SERIES_ATTEMPTS_BEFORE_COOL_DOWN,
        util::DEFAULT_CONNECT_DELAY,
        util::DEFAULT_CONNECT_SERIES_DELAY,
        util::DEFAULT_COOL_DOWN,
    );

    let client = Client::new(Vec::new());
    assert_eq!(delay_generator(&client, 1, 3), util::DEFAULT_CONNECT_DELAY);
    assert_eq!(delay_generator(&client, 2, 3), util::DEFAULT_CONNECT_DELAY);
    assert_eq!(
        delay_generator(&client, 3, 3),
        util::DEFAULT_CONNECT_SERIES_DELAY
    );
    assert_eq!(delay_generator(&client, 4, 3), util::DEFAULT_CONNECT_DELAY);
    assert_eq!(delay_generator(&client, 5, 3), util::DEFAULT_CONNECT_DELAY);
    assert_eq!(
        delay_generator(&client, 6, 3),
        util::DEFAULT_CONNECT_SERIES_DELAY
    );
    assert_eq!(delay_generator(&client, 7, 3), util::DEFAULT_CONNECT_DELAY);
    assert_eq!(delay_generator(&client, 8, 3), util::DEFAULT_CONNECT_DELAY);
    assert_eq!(delay_generator(&client, 9, 3), util::DEFAULT_COOL_DOWN);
    assert_eq!(delay_generator(&client, 10, 3), util::DEFAULT_CONNECT_DELAY);
    assert_eq!(delay_generator(&client, 11, 3), util::DEFAULT_CONNECT_DELAY);
    assert_eq!(
        delay_generator(&client, 12, 3),
        util::DEFAULT_CONNECT_SERIES_DELAY
    );
}

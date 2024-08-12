use boom::coordinates;

#[test]
fn test_radec2lb() {
    let ra = 323.233462;
    let dec = 14.112528;
    let (l, b) = coordinates::radec2lb(ra, dec);
    assert_eq!(l, 67.20337017520869);
    assert_eq!(b, -26.6109589874462);
}

#[test]
fn test_deg2hms() {
    let ra = 323.233462;
    let hms = coordinates::deg2hms(ra);
    assert_eq!(hms, "21:32:56.0309");
}

#[test]
fn test_deg2dms() {
    let dec = 14.112528;
    let dms = coordinates::deg2dms(dec);
    assert_eq!(dms, "14:06:45.101");
}
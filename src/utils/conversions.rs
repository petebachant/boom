pub fn flux2mag(flux: f32, flux_err: f32, zp: f32) -> (f32, f32) {
    let mag = -2.5 * (flux).log10() + zp;
    let sigma = (2.5 / 10.0_f32.ln()) * (flux_err / flux);

    (mag, sigma)
}

pub fn fluxerr2diffmaglim(flux_err: f32, zp: f32) -> f32 {
    -2.5 * (5.0 * flux_err).log10() + zp
}

use super::{estimate_cost_usd, should_write_gateway_error_fallback};

/// 函数 `assert_close`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - actual: 参数 actual
/// - expected: 参数 expected
///
/// # 返回
/// 无
fn assert_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 1e-12,
        "actual={actual}, expected={expected}"
    );
}

/// 函数 `estimate_cost_matches_openai_gpt5_family_prices`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn estimate_cost_matches_openai_gpt5_family_prices() {
    // 基准样本：输入 1000，缓存 200，输出 500
    // gpt-5 系列：输入 1.25/M，缓存 0.125/M，输出 10/M
    // => 非缓存输入 800*0.00125/1000 + 缓存 200*0.000125/1000 + 输出 500*0.01/1000
    // => 0.006025
    let expected = 0.006025_f64;
    let models = [
        "gpt-5",
        "gpt-5-codex",
        "gpt-5.1",
        "gpt-5.1-codex",
        "gpt-5.1-codex-max",
    ];
    for model in models {
        let actual = estimate_cost_usd(Some(model), Some(1000), Some(200), Some(500));
        assert_close(actual, expected);
    }
}

/// 函数 `estimate_cost_matches_openai_gpt54_and_mini_prices`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn estimate_cost_matches_openai_gpt54_and_mini_prices() {
    // gpt-5.4-mini：输入 0.75/M，缓存 0.075/M，输出 4.5/M
    // 样本：输入 1000，缓存 200，输出 500 => 0.002865
    let actual = estimate_cost_usd(Some("gpt-5.4-mini"), Some(1000), Some(200), Some(500));
    assert_close(actual, 0.002865);

    // gpt-5.4：输入 2.5/M，缓存 0.25/M，输出 15/M
    // 样本：输入 1000，缓存 200，输出 500
    // => 非缓存输入 800*0.0025/1000 + 缓存 200*0.00025/1000 + 输出 500*0.015/1000
    // => 0.00955
    let actual = estimate_cost_usd(Some("gpt-5.4"), Some(1000), Some(200), Some(500));
    assert_close(actual, 0.00955);
}

/// 函数 `estimate_cost_matches_openai_gpt54_large_context_prices`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn estimate_cost_matches_openai_gpt54_large_context_prices() {
    // gpt-5.4：输入超过 272K 时，输入 5/M，缓存 0.5/M，输出 22.5/M
    // 样本：输入 300000，缓存 50000，输出 100000
    // => 非缓存输入 250000*0.005/1000 + 缓存 50000*0.0005/1000 + 输出 100000*0.0225/1000
    // => 3.525
    let actual = estimate_cost_usd(Some("gpt-5.4"), Some(300_000), Some(50_000), Some(100_000));
    assert_close(actual, 3.525);
}

/// 函数 `estimate_cost_matches_openai_gpt54_pro_prices`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn estimate_cost_matches_openai_gpt54_pro_prices() {
    // gpt-5.4-pro：输入 30/M，输出 180/M；无缓存折扣时按输入同价处理。
    let actual = estimate_cost_usd(Some("gpt-5.4-pro"), Some(1000), Some(200), Some(500));
    assert_close(actual, 0.12);
}

/// 函数 `estimate_cost_matches_openai_gpt54_pro_large_context_prices`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn estimate_cost_matches_openai_gpt54_pro_large_context_prices() {
    // gpt-5.4-pro：输入超过 272K 时，输入 60/M，输出 270/M。
    let actual = estimate_cost_usd(
        Some("gpt-5.4-pro"),
        Some(300_000),
        Some(50_000),
        Some(100_000),
    );
    assert_close(actual, 45.0);
}

/// 函数 `estimate_cost_matches_openai_gpt5_mini_and_52_prices`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn estimate_cost_matches_openai_gpt5_mini_and_52_prices() {
    // mini：输入 0.25/M，缓存 0.025/M，输出 2/M
    // 样本同上 => 0.001205
    let mini_models = ["gpt-5.1-codex-mini", "gpt-5-codex-mini", "gpt-5-mini"];
    for model in mini_models {
        let mini_cost = estimate_cost_usd(Some(model), Some(1000), Some(200), Some(500));
        assert_close(mini_cost, 0.001205);
    }

    // 5.2：输入 1.75/M，缓存 0.175/M，输出 14/M
    // 样本同上 => 0.008435
    let v52_models = ["gpt-5.2", "gpt-5.2-codex"];
    for model in v52_models {
        let actual = estimate_cost_usd(Some(model), Some(1000), Some(200), Some(500));
        assert_close(actual, 0.008435);
    }
}

/// 函数 `estimate_cost_uses_cached_input_rate_for_gpt_5_1_codex`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn estimate_cost_uses_cached_input_rate_for_gpt_5_1_codex() {
    // 非缓存输入 800k * 1.25 + 缓存输入 200k * 0.125 + 输出 500k * 10
    // 期望：1 + 0.025 + 5 = 6.025 USD
    let actual = estimate_cost_usd(
        Some("gpt-5.1-codex"),
        Some(1_000_000),
        Some(200_000),
        Some(500_000),
    );
    assert_close(actual, 6.025);
}

/// 函数 `estimate_cost_falls_back_gpt_5_3_codex_to_gpt_5_2_codex_price`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn estimate_cost_falls_back_gpt_5_3_codex_to_gpt_5_2_codex_price() {
    // gpt-5.3-codex 暂按 gpt-5.2-codex：输入 1.75 + 输出 14.00
    let actual = estimate_cost_usd(
        Some("gpt-5.3-codex"),
        Some(1_000_000),
        Some(0),
        Some(1_000_000),
    );
    assert_close(actual, 15.75);
}

#[test]
fn gateway_error_fallback_matches_cloudflare_and_rate_limit_errors() {
    assert!(should_write_gateway_error_fallback(
        Some(403),
        Some("Cloudflare 安全验证页（title=Just a moment...）"),
    ));
    assert!(should_write_gateway_error_fallback(
        Some(429),
        Some("type=usage_limit_reached The usage limit"),
    ));
    assert!(!should_write_gateway_error_fallback(
        Some(500),
        Some("internal server error"),
    ));
}

/// 函数 `estimate_cost_matches_openai_gpt4o_and_o3_prices`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn estimate_cost_matches_openai_gpt4o_and_o3_prices() {
    let gpt41_models = ["gpt-4.1", "gpt-4.1-mini", "gpt-4.1-nano"];
    let gpt41_expected = [0.0057_f64, 0.00114_f64, 0.000285_f64];
    for (model, expected) in gpt41_models.into_iter().zip(gpt41_expected) {
        let actual = estimate_cost_usd(Some(model), Some(1000), Some(200), Some(500));
        assert_close(actual, expected);
    }

    let gpt4o_models = ["gpt-4o", "gpt-4o-mini", "gpt-4o-2024-05-13"];
    let gpt4o_expected = [0.00725_f64, 0.000435_f64, 0.0125_f64];
    for (model, expected) in gpt4o_models.into_iter().zip(gpt4o_expected) {
        let actual = estimate_cost_usd(Some(model), Some(1000), Some(200), Some(500));
        assert_close(actual, expected);
    }

    let o3_models = ["o3", "o3-mini", "o3-deep-research", "o3-pro"];
    let o3_expected = [0.0057_f64, 0.00319_f64, 0.0285_f64, 0.06_f64];
    for (model, expected) in o3_models.into_iter().zip(o3_expected) {
        let actual = estimate_cost_usd(Some(model), Some(1000), Some(200), Some(500));
        assert_close(actual, expected);
    }
}

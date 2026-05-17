use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(super) struct AuthorLinkItem {
    pub key: String,
    pub name: String,
    pub description: String,
    pub href: String,
    pub action_label: String,
    pub image_src: Option<String>,
    pub image_alt: Option<String>,
}

fn trim_text(value: &str) -> String {
    value.trim().to_string()
}

fn trim_optional_text(value: Option<String>) -> Option<String> {
    value
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty())
}

fn author_link_item(
    key: &str,
    name: &str,
    description: &str,
    href: &str,
    action_label: &str,
    image_src: Option<&str>,
    image_alt: Option<&str>,
) -> AuthorLinkItem {
    AuthorLinkItem {
        key: key.to_string(),
        name: name.to_string(),
        description: description.to_string(),
        href: href.to_string(),
        action_label: action_label.to_string(),
        image_src: image_src.map(ToString::to_string),
        image_alt: image_alt.map(ToString::to_string),
    }
}

pub(super) fn default_author_sponsors() -> Vec<AuthorLinkItem> {
    vec![
        author_link_item(
            "visioncoder",
            "VisionCoder",
            "VisionCoder 是一款高颜值、可灵活切换模型的桌面 AI 编程工具。它支持 Claude、Gemini、GPT，并集成 Claude Code、Gemini CLI、Codex、OpenCode 等多种 CLI 能力。",
            "https://coder.visioncoder.cn",
            "访问官网",
            Some("https://coder.visioncoder.cn/logo.png"),
            Some("VisionCoder"),
        ),
        author_link_item(
            "xingsiyan",
            "星思研中转站",
            "星思研中转站为 Claude Code、Codex、Gemini 等模型调用场景提供稳定中转与配套服务，适合需要高可用接口、便捷接入和持续交付支持的开发者与团队。",
            "https://gzxsy.vip/register?aff=eapz",
            "立即注册",
            Some("/sponsors/xingsiyan.jpg"),
            Some("星思研中转站"),
        ),
    ]
}

pub(super) fn default_author_server_recommendations() -> Vec<AuthorLinkItem> {
    vec![author_link_item(
        "racknerd",
        "RackNerd",
        "适合部署 CodexManager、网关转发服务和常规开发环境的 VPS 选择，适合需要稳定海外节点和可控成本的个人开发者或小团队。",
        "https://my.racknerd.com/aff.php?aff=19058",
        "查看套餐",
        Some("https://racknerd.com/banners/125x125.gif"),
        Some("RackNerd Square Banner"),
    )]
}

pub(super) fn normalize_author_link_items(items: Vec<AuthorLinkItem>) -> Vec<AuthorLinkItem> {
    items
        .into_iter()
        .enumerate()
        .map(|(index, item)| AuthorLinkItem {
            key: {
                let normalized = trim_text(&item.key);
                if normalized.is_empty() {
                    format!("item-{}", index + 1)
                } else {
                    normalized
                }
            },
            name: trim_text(&item.name),
            description: trim_text(&item.description),
            href: trim_text(&item.href),
            action_label: trim_text(&item.action_label),
            image_src: trim_optional_text(item.image_src),
            image_alt: trim_optional_text(item.image_alt),
        })
        .collect()
}

pub(super) fn load_author_link_items(
    settings: &HashMap<String, String>,
    key: &str,
    defaults: &[AuthorLinkItem],
) -> Vec<AuthorLinkItem> {
    settings
        .get(key)
        .and_then(|raw| serde_json::from_str::<Vec<AuthorLinkItem>>(raw).ok())
        .map(normalize_author_link_items)
        .unwrap_or_else(|| defaults.to_vec())
}

pub(super) fn serialize_author_link_items(items: &[AuthorLinkItem]) -> Result<String, String> {
    serde_json::to_string(items).map_err(|err| format!("serialize author links failed: {err}"))
}

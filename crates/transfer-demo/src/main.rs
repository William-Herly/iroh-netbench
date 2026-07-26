//! Native entry point for the transfer and network benchmark demo.

#![cfg_attr(target_os = "windows", windows_subsystem = "windows")]

use eframe::egui;
use transfer_demo::{DemoApp, init_desktop_file_logging};

fn main() -> eframe::Result {
    if let Err(error) = init_desktop_file_logging() {
        eprintln!("无法创建客户端诊断日志：{error}");
    }
    let options = eframe::NativeOptions {
        centered: true,
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([760.0, 680.0])
            .with_min_inner_size([620.0, 520.0]),
        ..Default::default()
    };

    eframe::run_native(
        "Iroh Transfer Demo",
        options,
        Box::new(|context| Ok(Box::new(DemoApp::new(context)))),
    )
}

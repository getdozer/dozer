use crate::console_helper::get_colored_text;
use crate::console_helper::GREEN;
use crate::console_helper::YELLOW;
use dozer_types::indicatif::ProgressBar;

pub struct ProgressPrinter {
    pb: ProgressBar,
}

impl ProgressPrinter {
    pub fn new() -> Self {
        Self {
            pb: ProgressBar::new_spinner(),
        }
    }

    pub fn start_step(&mut self, step_no: u32, text: &str) {
        self.pb.println("");

        self.pb.set_message(format!(
            "[{}] {}",
            step_no + 1,
            get_colored_text(text, YELLOW)
        ));
    }

    pub fn complete_step(&mut self, step_no: u32, text: &str) {
        self.pb.println(format!(
            " ✅ [{}] {}",
            step_no + 1,
            get_colored_text(text, GREEN)
        ));

        self.pb.set_message("".to_string());
    }
}

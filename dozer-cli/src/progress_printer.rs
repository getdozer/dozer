use crate::console_helper::GREEN;
use crate::console_helper::YELLOW;
use crate::painted;
use dozer_types::indicatif::ProgressBar;

pub struct ProgressPrinter {
    pb: ProgressBar,
    step_no: usize,
    steps: Vec<Step>,
    current_step: Option<Step>,
}

#[derive(Clone)]
pub struct Step {
    in_progress_text: String,
    completed_text: String,
}

pub fn get_deploy_steps() -> Vec<Step> {
    vec![
        Step {
            in_progress_text: "Creating application".to_string(),
            completed_text: "Application created".to_string(),
        },
        Step {
            in_progress_text: "Creating namespace".to_string(),
            completed_text: "Namespace created".to_string(),
        },
        Step {
            in_progress_text: "Creating data storage".to_string(),
            completed_text: "Data storage created".to_string(),
        },
        Step {
            in_progress_text: "Preparing application".to_string(),
            completed_text: "Application prepared".to_string(),
        },
        Step {
            in_progress_text: "Deploying app service".to_string(),
            completed_text: "App service deployed".to_string(),
        },
        Step {
            in_progress_text: "Starting app service".to_string(),
            completed_text: "App service started".to_string(),
        },
        Step {
            in_progress_text: "Deploying api service".to_string(),
            completed_text: "Api service deployed".to_string(),
        },
        Step {
            in_progress_text: "Starting api service".to_string(),
            completed_text: "Api service started".to_string(),
        },
        Step {
            in_progress_text: "Starting application".to_string(),
            completed_text: "Application started".to_string(),
        },
    ]
}

pub fn get_update_steps() -> Vec<Step> {
    vec![
        Step {
            in_progress_text: "Updating application".to_string(),
            completed_text: "Application updated".to_string(),
        },
        Step {
            in_progress_text: "Creating namespace".to_string(),
            completed_text: "Namespace created".to_string(),
        },
        Step {
            in_progress_text: "Creating data storage".to_string(),
            completed_text: "Data storage created".to_string(),
        },
        Step {
            in_progress_text: "Preparing application".to_string(),
            completed_text: "Application prepared".to_string(),
        },
        Step {
            in_progress_text: "Deploying app service".to_string(),
            completed_text: "App service deployed".to_string(),
        },
        Step {
            in_progress_text: "Starting app service".to_string(),
            completed_text: "App service started".to_string(),
        },
        Step {
            in_progress_text: "Deploying api service".to_string(),
            completed_text: "Api service deployed".to_string(),
        },
        Step {
            in_progress_text: "Starting api service".to_string(),
            completed_text: "Api service started".to_string(),
        },
        Step {
            in_progress_text: "Starting application".to_string(),
            completed_text: "Application started".to_string(),
        },
    ]
}

pub fn get_delete_steps() -> Vec<Step> {
    vec![
        Step {
            in_progress_text: "Stopping application".to_string(),
            completed_text: "Application stopped".to_string(),
        },
        Step {
            in_progress_text: "Deleting application".to_string(),
            completed_text: "Application deleted".to_string(),
        },
    ]
}

impl ProgressPrinter {
    pub fn new(steps: Vec<Step>) -> Self {
        Self {
            pb: ProgressBar::new_spinner(),
            step_no: 0_usize,
            steps,
            current_step: None,
        }
    }

    pub fn start_next_step(&mut self) {
        match &self.current_step {
            None => {}
            Some(step) => {
                self.pb.println(format!(
                    " ✅ [{}] {}",
                    self.step_no,
                    painted!(&step.completed_text, GREEN)
                ));
            }
        }

        let step_info = self.steps.get(self.step_no);

        self.step_no += 1;

        if let Some(step) = step_info {
            self.pb.set_message(format!(
                "[{}] {}",
                self.step_no,
                painted!(&step.in_progress_text, YELLOW)
            ));
            self.current_step = Some(step.clone());
        }
    }

    pub fn complete_step(&mut self, custom_text: Option<&str>) {
        let text = custom_text.unwrap_or({
            if let Some(current_step) = &self.current_step {
                &current_step.completed_text
            } else {
                ""
            }
        });

        self.pb
            .println(format!(" ✅ [{}] {}", self.step_no, painted!(text, GREEN)));
        self.current_step = None;
        self.pb.set_message("".to_string());
    }
}

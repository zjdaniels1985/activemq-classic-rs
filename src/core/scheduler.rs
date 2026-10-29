//! Scheduled message delivery (ActiveMQ `schedulerSupport`).
//!
//! Supports the `AMQ_SCHEDULED_DELAY`, `AMQ_SCHEDULED_PERIOD`,
//! `AMQ_SCHEDULED_REPEAT` and `AMQ_SCHEDULED_CRON` message properties.

use super::message::{Message, Value};
use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc};
use serde::{Deserialize, Serialize};

pub const SCHEDULED_DELAY: &str = "AMQ_SCHEDULED_DELAY";
pub const SCHEDULED_PERIOD: &str = "AMQ_SCHEDULED_PERIOD";
pub const SCHEDULED_REPEAT: &str = "AMQ_SCHEDULED_REPEAT";
pub const SCHEDULED_CRON: &str = "AMQ_SCHEDULED_CRON";
pub const SCHEDULED_ID: &str = "scheduledJobId";
pub const SCHEDULER_ACTION: &str = "AMQ_SCHEDULER_ACTION";
pub const SCHEDULER_ACTION_BROWSE: &str = "BROWSE";
pub const SCHEDULER_ACTION_REMOVE: &str = "REMOVE";
pub const SCHEDULER_ACTION_REMOVEALL: &str = "REMOVEALL";
pub const SCHEDULER_ACTION_START_TIME: &str = "ACTION_START_TIME";
pub const SCHEDULER_ACTION_END_TIME: &str = "ACTION_END_TIME";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub message: Message,
    pub cron: Option<String>,
    pub delay: i64,
    pub period: i64,
    /// Repeats configured on the message (-1 = forever).
    pub repeat: i32,
    /// Repeats remaining in the current cycle.
    pub remaining: i32,
    pub next_time: i64,
    pub created: i64,
}

/// Returns true when the message carries scheduling instructions.
pub fn is_scheduled(message: &Message) -> bool {
    if message.properties.contains_key(SCHEDULED_ID) {
        return false;
    }
    let delay = property_i64(message, SCHEDULED_DELAY).unwrap_or(0);
    let period = property_i64(message, SCHEDULED_PERIOD).unwrap_or(0);
    let repeat = property_i64(message, SCHEDULED_REPEAT).unwrap_or(0);
    let cron = message
        .properties
        .get(SCHEDULED_CRON)
        .and_then(Value::as_str)
        .is_some_and(|cron| !cron.trim().is_empty());
    delay > 0 || cron || (period > 0 && repeat != 0)
}

fn property_i64(message: &Message, name: &str) -> Option<i64> {
    message.properties.get(name).and_then(Value::as_i64)
}

impl Job {
    pub fn from_message(id: String, message: Message, now: i64) -> Result<Self, String> {
        let delay = property_i64(&message, SCHEDULED_DELAY).unwrap_or(0).max(0);
        let period = property_i64(&message, SCHEDULED_PERIOD).unwrap_or(0).max(0);
        let repeat = property_i64(&message, SCHEDULED_REPEAT)
            .unwrap_or(0)
            .clamp(-1, i64::from(i32::MAX)) as i32;
        let cron = message
            .properties
            .get(SCHEDULED_CRON)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|cron| !cron.is_empty())
            .map(str::to_owned);
        let next_time = match &cron {
            Some(expression) => {
                let schedule = CronSchedule::parse(expression)?;
                schedule
                    .next_after(now)
                    .ok_or_else(|| format!("cron expression '{expression}' never fires"))?
                    + delay
            }
            None => now + delay,
        };
        Ok(Self {
            id,
            message,
            cron,
            delay,
            period,
            repeat,
            remaining: repeat,
            next_time,
            created: now,
        })
    }

    /// Advances the job after it fired. Returns false when the job is finished.
    pub fn advance(&mut self, fired_at: i64) -> bool {
        if self.remaining != 0 && self.period > 0 {
            if self.remaining > 0 {
                self.remaining -= 1;
            }
            self.next_time = fired_at + self.period;
            return true;
        }
        if self.remaining != 0 && self.period == 0 && self.cron.is_none() {
            // Repeat without period: fire again immediately.
            if self.remaining > 0 {
                self.remaining -= 1;
            }
            self.next_time = fired_at;
            return true;
        }
        if let Some(expression) = &self.cron {
            if let Ok(schedule) = CronSchedule::parse(expression)
                && let Some(next) = schedule.next_after(fired_at.max(self.next_time))
            {
                self.remaining = self.repeat;
                self.next_time = next + self.delay;
                return true;
            }
        }
        false
    }
}

/// Five-field cron expressions (`minute hour day-of-month month day-of-week`), UTC.
#[derive(Clone, Debug)]
pub struct CronSchedule {
    minutes: Vec<bool>,
    hours: Vec<bool>,
    days_of_month: Vec<bool>,
    months: Vec<bool>,
    days_of_week: Vec<bool>,
    any_day_of_month: bool,
    any_day_of_week: bool,
}

impl CronSchedule {
    pub fn parse(expression: &str) -> Result<Self, String> {
        let fields = expression.split_whitespace().collect::<Vec<_>>();
        if fields.len() != 5 {
            return Err(format!(
                "cron expression '{expression}' must have 5 fields (minute hour day month weekday)"
            ));
        }
        let days_of_week = parse_field(fields[4], 0, 7)?;
        let mut normalized_dow = days_of_week[..7].to_vec();
        if days_of_week[7] {
            normalized_dow[0] = true;
        }
        Ok(Self {
            minutes: parse_field(fields[0], 0, 59)?,
            hours: parse_field(fields[1], 0, 23)?,
            days_of_month: parse_field(fields[2], 1, 31)?,
            months: parse_field(fields[3], 1, 12)?,
            days_of_week: normalized_dow,
            any_day_of_month: fields[2] == "*" || fields[2] == "?",
            any_day_of_week: fields[4] == "*" || fields[4] == "?",
        })
    }

    /// Next firing time strictly after `after` (milliseconds since epoch).
    pub fn next_after(&self, after: i64) -> Option<i64> {
        let start = Utc.timestamp_millis_opt(after).single()?;
        let mut candidate = start
            .with_second(0)?
            .with_nanosecond(0)?
            .checked_add_signed(Duration::minutes(1))?;
        let limit = start.checked_add_signed(Duration::days(366 * 5))?;
        while candidate <= limit {
            if !self.months[candidate.month() as usize] {
                candidate = first_of_next_month(candidate)?;
                continue;
            }
            if !self.day_matches(&candidate) {
                candidate = (candidate + Duration::days(1))
                    .with_hour(0)?
                    .with_minute(0)?;
                continue;
            }
            if !self.hours[candidate.hour() as usize] {
                candidate = (candidate + Duration::hours(1)).with_minute(0)?;
                continue;
            }
            if !self.minutes[candidate.minute() as usize] {
                candidate += Duration::minutes(1);
                continue;
            }
            return Some(candidate.timestamp_millis());
        }
        None
    }

    fn day_matches(&self, time: &DateTime<Utc>) -> bool {
        let dom = self.days_of_month[time.day() as usize];
        let dow = self.days_of_week[time.weekday().num_days_from_sunday() as usize];
        match (self.any_day_of_month, self.any_day_of_week) {
            (true, true) => true,
            (true, false) => dow,
            (false, true) => dom,
            (false, false) => dom || dow,
        }
    }
}

fn first_of_next_month(time: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let (year, month) = if time.month() == 12 {
        (time.year() + 1, 1)
    } else {
        (time.year(), time.month() + 1)
    };
    Utc.with_ymd_and_hms(year, month, 1, 0, 0, 0).single()
}

fn parse_field(field: &str, min: u32, max: u32) -> Result<Vec<bool>, String> {
    let mut allowed = vec![false; max as usize + 1];
    for part in field.split(',') {
        let (range, step) = match part.split_once('/') {
            Some((range, step)) => (
                range,
                step.parse::<u32>()
                    .ok()
                    .filter(|step| *step > 0)
                    .ok_or_else(|| format!("invalid cron step in '{part}'"))?,
            ),
            None => (part, 1),
        };
        let (start, end) = if range == "*" || range == "?" {
            (min, max)
        } else if let Some((start, end)) = range.split_once('-') {
            (parse_number(start, min, max)?, parse_number(end, min, max)?)
        } else {
            let value = parse_number(range, min, max)?;
            (value, if part.contains('/') { max } else { value })
        };
        if start > end {
            return Err(format!("invalid cron range '{part}'"));
        }
        let mut value = start;
        while value <= end {
            allowed[value as usize] = true;
            value += step;
        }
    }
    Ok(allowed)
}

fn parse_number(value: &str, min: u32, max: u32) -> Result<u32, String> {
    value
        .parse::<u32>()
        .ok()
        .filter(|number| (min..=max).contains(number))
        .ok_or_else(|| format!("cron value '{value}' must be between {min} and {max}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::destination::Destination;

    fn at(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> i64 {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
            .unwrap()
            .timestamp_millis()
    }

    #[test]
    fn cron_finds_next_occurrence() {
        let every_five = CronSchedule::parse("*/5 * * * *").unwrap();
        assert_eq!(
            every_five.next_after(at(2026, 1, 1, 10, 2)),
            Some(at(2026, 1, 1, 10, 5))
        );
        let daily = CronSchedule::parse("30 6 * * *").unwrap();
        assert_eq!(
            daily.next_after(at(2026, 1, 1, 7, 0)),
            Some(at(2026, 1, 2, 6, 30))
        );
        let mondays = CronSchedule::parse("0 9 * * 1").unwrap();
        // 2026-01-01 is a Thursday; the next Monday is 2026-01-05.
        assert_eq!(
            mondays.next_after(at(2026, 1, 1, 0, 0)),
            Some(at(2026, 1, 5, 9, 0))
        );
        let yearly = CronSchedule::parse("0 0 1 3 *").unwrap();
        assert_eq!(
            yearly.next_after(at(2026, 4, 1, 0, 0)),
            Some(at(2027, 3, 1, 0, 0))
        );
        assert!(CronSchedule::parse("61 * * * *").is_err());
        assert!(CronSchedule::parse("* * *").is_err());
    }

    #[test]
    fn delay_period_and_repeat() {
        let message = Message::text(Destination::queue("q"), "x")
            .with_property(SCHEDULED_DELAY, 1000_i64)
            .with_property(SCHEDULED_PERIOD, 500_i64)
            .with_property(SCHEDULED_REPEAT, 2);
        assert!(is_scheduled(&message));
        let mut job = Job::from_message("job".to_owned(), message, 10_000).unwrap();
        assert_eq!(job.next_time, 11_000);
        assert!(job.advance(11_000));
        assert_eq!(job.next_time, 11_500);
        assert!(job.advance(11_500));
        assert_eq!(job.next_time, 12_000);
        assert!(!job.advance(12_000));
    }

    #[test]
    fn already_fired_messages_are_not_rescheduled() {
        let message = Message::text(Destination::queue("q"), "x")
            .with_property(SCHEDULED_DELAY, 1000_i64)
            .with_property(SCHEDULED_ID, "job");
        assert!(!is_scheduled(&message));
    }
}

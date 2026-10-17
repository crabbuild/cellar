use super::*;

impl Workflow {
    pub(crate) async fn create_scheduled(
        &self,
        context: &Context,
        schedule: CronSchedule,
    ) -> Result<()> {
        let id = schedule.instance_id();
        let (instance, mut arguments) = self.options(CreateOptions {
            id: Some(id),
            ..Default::default()
        })?;
        set(
            &mut arguments,
            "schedule",
            &StructuredValue::from_json(serde_json::to_value(schedule)?)?,
        )?;
        set(
            &mut arguments,
            "skipExisting",
            &StructuredValue::from_json(json!(true))?,
        )?;
        instance
            .call(context, "__wfCreate", Some(arguments))
            .await?;
        Ok(())
    }
}

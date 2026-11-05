
variable "scoring" {
  description = "Umbrales del scoring por rangos (modelo RULES_V1). Ver flink-sql/21-insert-loan-decisions.sql.tftpl."
  type = object({
    model             = string
    approve_threshold = number # puntos mínimos (0-100) para aprobar
    dti_good          = number # ratio (deuda + cuota) / ingresos
    dti_ok            = number
    dti_max           = number
    max_defaulted     = number # impagos tolerados
    late_max          = number # retrasos tolerados con buena puntuación
    emp_long          = number # años de antigüedad laboral
    emp_mid           = number
    emp_min           = number
    amt_low           = number # importe / ingresos anuales
    amt_high          = number
  })
  default = {
    model             = "RULES_V1"
    approve_threshold = 60
    dti_good          = 0.30
    dti_ok            = 0.40
    dti_max           = 0.50
    max_defaulted     = 0
    late_max          = 2
    emp_long          = 5
    emp_mid           = 3
    emp_min           = 1
    amt_low           = 0.5
    amt_high          = 1.0
  }

  validation {
    condition     = var.scoring.dti_good <= var.scoring.dti_ok && var.scoring.dti_ok <= var.scoring.dti_max && var.scoring.emp_min <= var.scoring.emp_mid && var.scoring.emp_mid <= var.scoring.emp_long && var.scoring.amt_low <= var.scoring.amt_high
    error_message = "Los umbrales deben ser crecientes (dti_good <= dti_ok <= dti_max, emp_min <= emp_mid <= emp_long, amt_low <= amt_high)."
  }

  validation {
    condition     = var.scoring.approve_threshold >= 0 && var.scoring.approve_threshold <= 100 && can(regex("^[A-Z0-9_]{1,30}$", var.scoring.model))
    error_message = "approve_threshold debe estar entre 0 y 100 y model solo admite A-Z, 0-9 y _."
  }
}

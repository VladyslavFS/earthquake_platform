import logging
from typing import List, Dict
from dataclasses import dataclass

from core.connectors import BaseConnector


@dataclass
class DataQualityRule:
    """Data quality rule definition"""
    name: str
    query: str
    threshold: float
    operator: str  # '>', '<', '==', '>=', '<='


class DataQualityChecker:
    """Validates data quality across layers"""
    
    def __init__(self, connector: BaseConnector):
        self.connector = connector
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def check_rule(self, rule: DataQualityRule) -> bool:
        """Execute a single quality rule"""
        result = self.connector.execute(rule.query)
        value = result[0][0] if result else None
        
        if value is None:
            self.logger.warning(f"Rule {rule.name} returned no value")
            return False
        
        passed = self._evaluate_rule(value, rule.threshold, rule.operator)
        
        if not passed:
            self.logger.error(
                f"Quality check FAILED: {rule.name} "
                f"(value: {value}, threshold: {rule.threshold})"
            )
        return passed
    
    def _evaluate_rule(self, value: float, threshold: float, operator: str) -> bool:
        """Evaluate rule based on operator"""
        operators = {
            '>': lambda v, t: v > t,
            '<': lambda v, t: v < t,
            '==': lambda v, t: v == t,
            '>=': lambda v, t: v >= t,
            '<=': lambda v, t: v <= t,
        }
        return operators.get(operator, lambda v, t: False)(value, threshold)
    
    def run_checks(self, rules: List[DataQualityRule]) -> Dict[str, bool]:
        """Run multiple quality checks"""
        results = {}
        for rule in rules:
            results[rule.name] = self.check_rule(rule)
        return results
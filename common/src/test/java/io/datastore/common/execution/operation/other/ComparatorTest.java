package io.datastore.common.execution.operation.other;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class ComparatorTest {

  private static final String LESSER_VALUE = "AA";
  private static final String GREATER_VALUE = "ABC";
  private static final String COMPARISON_FUNCTION = "function (a, b) {return a.length - b.length;}";
  private static final ScriptEngine SCRIPT_ENGINE = new ScriptEngineManager().getEngineByName("nashorn");

  private Comparator comparator;

  @Before
  public void setUp() {
    comparator = new Comparator();
    comparator.with(SCRIPT_ENGINE, COMPARISON_FUNCTION);
  }

  @Test
  public void shouldReturnNegativeValueWhenFirstArgumentIsSmallerThanSecond() {
    assertThat(comparator.apply(LESSER_VALUE, GREATER_VALUE)).isLessThan(0);
  }

  @Test
  public void shouldReturnPositiveValueWhenFirstArgumentIsBiggerThanSecond() {
    assertThat(comparator.apply(GREATER_VALUE, LESSER_VALUE)).isGreaterThan(0);
  }
}
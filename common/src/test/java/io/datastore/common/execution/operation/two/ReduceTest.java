package io.datastore.common.execution.operation.two;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class ReduceTest {

  private static final String INTERMEDIATE_VALUE = "AA";
  private static final String INPUT_DATA = "ABC";
  private static final String OUTPUT_VALUE = INTERMEDIATE_VALUE + INPUT_DATA;
  private static final String REDUCTION_FUNCTION = "function (a, b) {return a + b;}";
  private static final ScriptEngine SCRIPT_ENGINE = new ScriptEngineManager().getEngineByName("nashorn");

  private Reduce reduce;

  @Before
  public void setUp() {
    reduce = new Reduce();
    reduce.with(SCRIPT_ENGINE, REDUCTION_FUNCTION);
  }

  @Test
  public void shouldReturnOutputValueWhenApplied() {
    assertThat(reduce.apply(INTERMEDIATE_VALUE, INPUT_DATA)).isEqualTo(OUTPUT_VALUE);
  }
}
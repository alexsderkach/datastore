package io.datastore.common.execution.operation.one;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class FilterTest {
  private static final String VALID_DATA = "A";
  private static final String INVALID_DATA = "ABC";
  private static final String FILTERING_FUNCTION = "function (a) {return a.length < 2;}";
  private static final ScriptEngine SCRIPT_ENGINE = new ScriptEngineManager().getEngineByName("nashorn");

  private Filter filter;

  @Before
  public void setUp() {
    filter = new Filter();
    filter.with(SCRIPT_ENGINE, FILTERING_FUNCTION);
  }

  @Test
  public void shouldReturnDataWhenDataIsValid() {
    assertThat(filter.apply(VALID_DATA)).isEqualTo(VALID_DATA);
  }

  @Test
  public void shouldReturnNullWhenDataIsValid() {
    assertThat(filter.apply(INVALID_DATA)).isNull();
  }
}
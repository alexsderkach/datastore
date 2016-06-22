package io.datastore.common.execution.operation.one;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class MapTest {
  private static final String INPUT_DATA = "A";
  private static final String OUTPUT_DATA = "ABC";
  private static final String MAPPING_FUNCTION = "function (a) {return a + 'BC';}";
  private static final ScriptEngine SCRIPT_ENGINE = new ScriptEngineManager().getEngineByName("nashorn");

  private Map map;

  @Before
  public void setUp() {
    map = new Map();
    map.with(SCRIPT_ENGINE, MAPPING_FUNCTION);
  }

  @Test
  public void shouldApplyMappingFunctionWhenCalledApply() {
    assertThat(map.apply(INPUT_DATA)).isEqualTo(OUTPUT_DATA);
  }
}
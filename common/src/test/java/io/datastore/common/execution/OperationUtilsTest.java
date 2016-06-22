package io.datastore.common.execution;

import io.datastore.common.execution.operation.Operation1;
import io.datastore.common.execution.operation.Operation2;
import io.datastore.common.execution.operation.other.Comparator;
import io.datastore.common.execution.stream.DataStream;
import io.datastore.common.execution.stream.TextDataStream;
import io.vertx.rxjava.core.http.HttpServerResponse;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.StringJoiner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class OperationUtilsTest {

  public static final String KEY = "key";
  public static final String ACCEPTANCE_VALUE = "value1";
  public static final String ACCEPTANCE_INPUT_DATA = ACCEPTANCE_VALUE + "\n";
  public static final String ACCEPTANCE_OPERATION_RESULT = "result1";
  public static final String ACCEPTANCE_OUTPUT_DATA = ACCEPTANCE_OPERATION_RESULT + "\n";


  public static final String REDUCTION_VALUE_1 = "value1";
  public static final String REDUCTION_VALUE_2 = "value2";
  public static final String REDUCTION_INPUT_DATA
      = new StringJoiner("\n").add(REDUCTION_VALUE_1).add(REDUCTION_VALUE_2).add("").toString();
  public static final String REDUCTION_OPERATION_RESULT = REDUCTION_VALUE_1 + REDUCTION_VALUE_2;
  public static final String REDUCTION_OUTPUT_DATA = REDUCTION_OPERATION_RESULT + "\n";

  public static final String SORTING_INPUT_DATA = new StringJoiner("\n").add("value3").add("value1").add("value2").add("").toString();
  public static final String SORTING_OUTPUT_DATA = new StringJoiner("\n").add("value1").add("value2").add("value3").add("").toString();

  @Mock
  private HttpServerResponse response;

  private DataStream dataStream;

  @Test
  public void shouldWriteValidResponseWhenCalledDoAccepting() {
    // given
    dataStream = new TextDataStream(ACCEPTANCE_INPUT_DATA, "");

    Operation1 operation = mock(Operation1.class);
    when(operation.apply(ACCEPTANCE_VALUE)).thenReturn(ACCEPTANCE_OPERATION_RESULT);

    // when
    OperationUtils.doAcceptance(response, KEY, dataStream, operation);

    // then
    verify(response).write(eq(ACCEPTANCE_OUTPUT_DATA));
  }

  @Test
  public void shouldReturnValidReductionResultWhenCalledDoReduction() {
    // given
    dataStream = new TextDataStream(REDUCTION_INPUT_DATA, "");

    Operation2 operation = mock(Operation2.class);
    when(operation.apply(eq(REDUCTION_VALUE_1), eq(REDUCTION_VALUE_2))).thenReturn(REDUCTION_OPERATION_RESULT);

    // when
    String result = OperationUtils.doReduction(dataStream, operation, null);

    // then
    assertThat(result).isEqualTo(REDUCTION_OUTPUT_DATA);
  }

  @Test
  public void shouldWriteValidResponseWhenCalledDoSorting() {
    // given
    dataStream = new TextDataStream(SORTING_INPUT_DATA, "");

    Comparator comparator = mock(Comparator.class);
    when(comparator.apply(any(), any())).thenAnswer(i -> {
      Object[] arguments = i.getArguments();
      String a = (String) arguments[0];
      String b = (String) arguments[1];
      return (double) String.CASE_INSENSITIVE_ORDER.compare(a, b);
    });

    // when
    OperationUtils.doSort(response, KEY, dataStream, comparator);

    // then
    verify(response).write(eq(SORTING_OUTPUT_DATA));
  }
}
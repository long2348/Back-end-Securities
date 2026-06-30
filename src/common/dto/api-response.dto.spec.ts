import { ApiResponse } from './api-response.dto';

describe('ApiResponse', () => {
  it('ok() returns success=true with data', () => {
    const res = ApiResponse.ok({ id: 1 });
    expect(res.success).toBe(true);
    expect(res.data).toEqual({ id: 1 });
    expect(res.message).toBe('Success');
  });

  it('fail() returns success=false with null data', () => {
    const res = ApiResponse.fail('Not found');
    expect(res.success).toBe(false);
    expect(res.data).toBeNull();
    expect(res.message).toBe('Not found');
  });

  it('ok() accepts custom message', () => {
    const res = ApiResponse.ok([], 'Empty list');
    expect(res.message).toBe('Empty list');
  });
});

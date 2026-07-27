import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { LoginPage } from './LoginPage';

vi.mock('../api/client', () => ({
  setToken: vi.fn(),
}));

vi.mock('../config/server', () => ({
  getApiBase: () => '',
  getServerUrl: () => '',
  setServerUrl: vi.fn(),
}));

describe('LoginPage registration', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  it('sends the optional bootstrap token with registration', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({ ok: true }),
      })
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          token: 'jwt-token',
          user: {
            id: 1,
            email: 'owner@example.com',
            name: 'Owner',
            role: 'super_admin',
          },
        }),
      });
    vi.stubGlobal('fetch', fetchMock);
    const onLogin = vi.fn();

    render(<LoginPage onLogin={onLogin} />);
    await userEvent.click(screen.getByRole('button', { name: 'Register' }));
    await userEvent.type(screen.getByPlaceholderText('Name'), 'Owner');
    await userEvent.type(
      screen.getByPlaceholderText('Email'),
      'owner@example.com',
    );
    await userEvent.click(screen.getByRole('button', { name: 'Send Code' }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));

    await userEvent.type(
      screen.getByPlaceholderText('Verification Code'),
      '123456',
    );
    await userEvent.type(screen.getByPlaceholderText('Password'), 'password');
    await userEvent.type(
      screen.getByPlaceholderText('Bootstrap Token (first admin only)'),
      'deployment-token',
    );
    const registerButtons = screen.getAllByRole('button', {
      name: 'Register',
    });
    fireEvent.submit(
      registerButtons[registerButtons.length - 1].closest('form')!,
    );

    await waitFor(() => expect(onLogin).toHaveBeenCalledOnce());
    const request = fetchMock.mock.calls[1][1];
    expect(JSON.parse(request.body)).toEqual({
      email: 'owner@example.com',
      name: 'Owner',
      password: 'password',
      code: '123456',
      bootstrap_token: 'deployment-token',
    });
  });
});
